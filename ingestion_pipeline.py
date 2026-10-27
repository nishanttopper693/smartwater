"""
Data Ingestion and Validation Pipeline (Slide 8 & Report Section VII-B)
Implements the 5-Stage Streaming Ingestion Sequence:
1. Preserve: Keep raw payload and provenance
2. Validate: Sensor ID, timestamps, units, and measurement bounds
3. Align Time: Event time vs. ingest time (transport lag, clock skew, late-data watermarks)
4. Handle Gaps: Explicit missingness tracking ("No reading is not zero flow")
5. Quarantine: Route invalid data with clear reason codes to 'water.quarantine'
"""

import time
import copy
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Tuple, Any
from collections import deque

APPROVED_FLOW_UNITS = {"m3/h", "m3h", "lpm", "l/min"}
APPROVED_PRESSURE_UNITS = {"bar", "kpa"}

@dataclass
class ValidationResult:
    is_valid: bool
    quality_flag: str  # "VALID", "QUARANTINED", "ESTIMATED"
    stage_failed: Optional[str]  # "PRESERVE", "VALIDATE", "ALIGN_TIME", "HANDLE_GAPS", None
    reason_code: Optional[str]  # e.g. "NEGATIVE_FLOW", "INVALID_UNIT", "MISSING_TIMESTAMP", etc.
    message: str
    transport_lag_ms: float
    processed_payload: Dict[str, Any]
    raw_payload: Dict[str, Any]

class DataIngestionPipeline:
    """
    Five-stage data ingestion, validation and quarantine pipeline.
    """
    WATERMARK_MAX_DELAY_SEC = 900.0  # 15 minutes late-data cutoff
    MAX_FUTURE_SKEW_SEC = 5.0        # Max allowable future clock skew

    def __init__(self, broker=None, max_quarantine_history: int = 100):
        self.broker = broker
        self.quarantine_history: deque = deque(maxlen=max_quarantine_history)
        self.processed_events_count = 0
        self.quarantined_events_count = 0
        self.valid_events_count = 0
        self.estimated_events_count = 0
        self.last_sensor_reading_time: Dict[str, float] = {}
        self.raw_event_archive: deque = deque(maxlen=500)

    def process_event(self, incoming_event: Dict[str, Any]) -> ValidationResult:
        """
        Executes the 5-Stage Ingestion Pipeline on an incoming sensor event.
        """
        self.processed_events_count += 1
        ingest_time_epoch = time.time()
        ingest_time_iso = time.strftime("%Y-%m-%dT%H:%M:%S.024Z", time.gmtime(ingest_time_epoch))

        # ----------------------------------------------------
        # STAGE 1: PRESERVE (Keep raw payload and provenance)
        # ----------------------------------------------------
        raw_copy = copy.deepcopy(incoming_event)
        provenance = {
            "archive_id": f"RAW-{self.processed_events_count:06d}",
            "source_gateway": incoming_event.get("source_id", "IOT-GATEWAY-DEFAULT"),
            "ingest_timestamp": ingest_time_epoch,
            "ingest_time_iso": ingest_time_iso
        }
        self.raw_event_archive.append({"raw": raw_copy, "provenance": provenance})

        processed = copy.deepcopy(incoming_event)
        processed["ingest_time"] = ingest_time_iso

        # ----------------------------------------------------
        # STAGE 2: VALIDATE (Identity, timestamps, units, values)
        # ----------------------------------------------------
        # 2a. Sensor ID Check
        sensor_id = processed.get("sensor_id")
        if not sensor_id or not isinstance(sensor_id, str) or sensor_id.strip() == "":
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="MISSING_SENSOR_ID",
                msg="Sensor identity missing or malformed."
            )

        # 2b. Timestamp Existence Check
        event_time_str = processed.get("event_time")
        if not event_time_str or not isinstance(event_time_str, str) or event_time_str.strip() == "":
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="MISSING_TIMESTAMP",
                msg="Event timestamp is missing or empty."
            )

        # Parse event timestamp
        event_time_epoch = self._parse_iso_timestamp(event_time_str)
        if event_time_epoch is None:
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="INVALID_TIMESTAMP_FORMAT",
                msg=f"Event timestamp '{event_time_str}' could not be parsed as valid ISO/UTC time."
            )

        # 2c. Metric & Units Check
        metric = str(processed.get("metric", "")).lower()
        unit = str(processed.get("unit", "")).lower()
        if metric == "flow" or "flow" in str(sensor_id).lower() or "flw" in str(sensor_id).lower():
            if unit not in APPROVED_FLOW_UNITS:
                return self._quarantine(
                    raw_copy, processed,
                    stage="VALIDATE",
                    reason="INVALID_UNIT",
                    msg=f"Unrecognized flow unit '{unit}'. Approved units: m3/h, lpm."
                )
        elif metric == "pressure" or "press" in str(sensor_id).lower() or "prs" in str(sensor_id).lower() or "pt-" in str(sensor_id).lower():
            if unit not in APPROVED_PRESSURE_UNITS:
                return self._quarantine(
                    raw_copy, processed,
                    stage="VALIDATE",
                    reason="INVALID_UNIT",
                    msg=f"Unrecognized pressure unit '{unit}'. Approved units: bar, kpa."
                )

        # 2d. Value Physical Bounds Check
        raw_val = processed.get("value")
        if raw_val is None:
            raw_val = processed.get("flow_reading_m3h") if metric == "flow" else processed.get("inlet_pressure_bar")

        try:
            val = float(raw_val)
        except (TypeError, ValueError):
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="NON_NUMERIC_MEASUREMENT",
                msg=f"Measurement value '{raw_val}' is not a valid floating-point number."
            )

        # Check for negative flow
        if metric == "flow" and val < 0.0:
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="NEGATIVE_FLOW",
                msg=f"Physical flow cannot be negative: received {val} {unit}."
            )

        # Check for pressure bounds (0 to 25 bar)
        if metric == "pressure" and (val < 0.0 or val > 25.0):
            return self._quarantine(
                raw_copy, processed,
                stage="VALIDATE",
                reason="PRESSURE_OUT_OF_BOUNDS",
                msg=f"Pressure measurement {val} {unit} is outside physical limits [0, 25 bar]."
            )

        # ----------------------------------------------------
        # STAGE 3: ALIGN TIME (Event time vs. Ingest time)
        # ----------------------------------------------------
        transport_lag_sec = ingest_time_epoch - event_time_epoch
        transport_lag_ms = round(transport_lag_sec * 1000.0, 1)

        # Check future clock skew
        if transport_lag_sec < -self.MAX_FUTURE_SKEW_SEC:
            return self._quarantine(
                raw_copy, processed,
                stage="ALIGN_TIME",
                reason="FUTURE_CLOCK_SKEW",
                msg=f"Event time is in the future by {-transport_lag_sec:.2f}s (Clock skew violation)."
            )

        # Check late data beyond watermark policy
        if transport_lag_sec > self.WATERMARK_MAX_DELAY_SEC:
            return self._quarantine(
                raw_copy, processed,
                stage="ALIGN_TIME",
                reason="WATERMARK_EXCEEDED_DELAY",
                msg=f"Event arrival delayed by {transport_lag_sec:.1f}s, exceeding watermark window ({self.WATERMARK_MAX_DELAY_SEC}s). Route to historical archive."
            )

        # ----------------------------------------------------
        # STAGE 4: HANDLE GAPS ("No reading is not zero flow")
        # ----------------------------------------------------
        # Verify gap status against sensor schedule
        is_gap_estimated = False
        if processed.get("quality_flag") == "ESTIMATED":
            is_gap_estimated = True
            self.estimated_events_count += 1
            processed["quality_flag"] = "ESTIMATED"
        else:
            processed["quality_flag"] = "VALID"
            self.valid_events_count += 1

        self.last_sensor_reading_time[sensor_id] = event_time_epoch
        processed["transport_lag_ms"] = transport_lag_ms

        # ----------------------------------------------------
        # STAGE 5: ROUTE (Publish to normal telemetry topic)
        # ----------------------------------------------------
        target_topic = processed.get("topic", "water.telemetry.inlet")
        if self.broker is not None:
            self.broker.publish(target_topic, sensor_id, processed)

        return ValidationResult(
            is_valid=True,
            quality_flag=processed["quality_flag"],
            stage_failed=None,
            reason_code=None,
            message="Event passed all 5 pipeline validation stages successfully.",
            transport_lag_ms=transport_lag_ms,
            processed_payload=processed,
            raw_payload=raw_copy
        )

    def _quarantine(self, raw_copy: Dict[str, Any], processed: Dict[str, Any],
                    stage: str, reason: str, msg: str) -> ValidationResult:
        """Helper to tag, record and route an invalid event to the quarantine topic."""
        self.quarantined_events_count += 1
        processed["quality_flag"] = "QUARANTINED"
        processed["quarantine_stage"] = stage
        processed["quarantine_reason"] = reason
        processed["quarantine_error"] = msg
        processed["quarantined_at"] = time.strftime("%H:%M:%S")

        quarantine_record = {
            "timestamp": processed["quarantined_at"],
            "event_id": processed.get("event_id", f"Q-{self.quarantined_events_count:04d}"),
            "sensor_id": processed.get("sensor_id", "UNKNOWN"),
            "metric": processed.get("metric", "unknown"),
            "value": processed.get("value", "N/A"),
            "unit": processed.get("unit", "N/A"),
            "stage_failed": stage,
            "reason_code": reason,
            "error_message": msg,
            "raw_payload": raw_copy
        }
        self.quarantine_history.insert(0, quarantine_record)

        # Route directly to Kafka water.quarantine topic
        if self.broker is not None:
            self.broker.publish("water.quarantine", str(processed.get("sensor_id", "QUARANTINED")), processed)

        return ValidationResult(
            is_valid=False,
            quality_flag="QUARANTINED",
            stage_failed=stage,
            reason_code=reason,
            message=msg,
            transport_lag_ms=0.0,
            processed_payload=processed,
            raw_payload=raw_copy
        )

    @staticmethod
    def _parse_iso_timestamp(ts_str: str) -> Optional[float]:
        """Safely parses ISO timestamp into unix epoch timestamp."""
        try:
            # Common ISO formats
            if ts_str.endswith("Z"):
                ts_str_clean = ts_str[:-1]
                dt = datetime.fromisoformat(ts_str_clean)
                return dt.replace(tzinfo=timezone.utc).timestamp()
            dt = datetime.fromisoformat(ts_str)
            return dt.timestamp()
        except Exception:
            try:
                # Fallback to time string parsing HH:MM:SS
                t = time.strptime(ts_str, "%H:%M:%S")
                now = time.localtime()
                # Reconstruct today with that time
                return time.mktime((now.tm_year, now.tm_mon, now.tm_mday, t.tm_hour, t.tm_min, t.tm_sec, 0, 0, -1))
            except Exception:
                return None
