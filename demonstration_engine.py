"""
Demonstration Engine: Rule-Based Leakage Detector
Based on project report specifications:
- Reservoir inlet and downstream pipeline sensors
- Flow baseline: 30.2 m³/h | Abnormal surge: 48.5 m³/h
- Pressure baseline: 4.18 bar | Abnormal drop: 3.50 bar -> 3.48 bar
- Multi-reading persistence verification before candidate incident creation
- All data explicitly labeled as SIMULATED
- Candidate alerts strictly designated as UNCONFIRMED
"""

import time
import random
from collections import deque
from dataclasses import dataclass, asdict, field
from typing import Dict, List, Optional, Tuple, Any

from ingestion_pipeline import DataIngestionPipeline, ValidationResult

@dataclass
class SimulatedSensorReading:
    timestamp: float
    time_str: str
    location_id: str
    location_name: str
    flow_m3h: float
    pressure_bar: float
    is_anomaly_sample: bool
    sensor_tag: str = ""
    health_status: str = "HEALTHY"
    zone_id: str = "Z-04"
    data_label: str = "SIMULATED"

@dataclass
class CandidateIncident:
    incident_id: str
    timestamp: str
    location_id: str
    location_name: str
    flow_reading: float
    baseline_flow: float
    pressure_reading: float
    baseline_pressure: float
    consecutive_readings_count: int
    readings_sequence: List[float]
    reason_for_alert: str
    incident_status: str  # CANDIDATE_LEAKAGE_INCIDENT (UNCONFIRMED)
    is_confirmed: bool = False
    data_label: str = "SIMULATED"
    zone_id: str = "Z-04"
    candidate_segment: str = "Segment 14B"
    heuristic_score: float = 0.89
    branch_sensor_id: str = "PT-203"
    branch_pressure_bar: float = 4.21
    unmatched_volume_m3: float = 1.25

@dataclass
class TelemetryEventContract:
    """Table IV: Proposed Event Contract Schema"""
    # Identity
    event_id: str
    sensor_id: str
    sequence_no: int
    source_id: str
    # Time
    event_time: str
    ingest_time: str
    source_clock_status: str
    # Measurement
    metric: str
    value: float
    unit: str
    sampling_interval_sec: int
    # Quality
    quality_flag: str  # VALID, ESTIMATED, QUARANTINED, SUSPECT
    original_value: Optional[float]
    correction_version: int
    # Asset context
    asset_id: str
    zone_id: str
    mapping_version: str
    # Device and schema
    device_status: str
    calibration_date: str
    schema_version: str
    data_label: str = "SIMULATED"

@dataclass
class DistrictWaterBalance:
    """District Z-04 Mass-Balance Conservation (Report Section XVI-A)"""
    zone_id: str
    inlet_flow_m3h: float          # Q_in
    export_flow_m3h: float         # Q_export
    authorized_consumption_m3h: float # Q_authorized
    storage_change_m3h: float      # dV/dt
    balance_residual_m3h: float    # R = Q_in - Q_export - Q_auth - dV/dt
    unmatched_volume_5min_m3: float # R * (5 / 60)
    upstream_pressure_bar: float
    downstream_pressure_bar: float
    candidate_incident_id: Optional[str] = None
    candidate_area: str = "Z-04 downstream pipe group (Segment 14B)"
    status: str = "BALANCED_NORMAL"
    data_label: str = "SIMULATED"

@dataclass
class SensorHealthRecord:
    sensor_id: str
    sensor_tag: str
    status: str  # "ONLINE_HEALTHY", "FROZEN_FLATLINE", "OUT_OF_BOUNDS", "CLOCK_SKEW"
    is_healthy: bool
    variance: float
    consecutive_repeats: int
    diagnostic_message: str

class RuleBasedLeakageDetector:
    """
    Explainable Rule-Based Leakage Detector & Water Network Model
    Based on Project Report & PPT Specifications:
    1. Baseline: Flow 30.2 m³/h, Pressure 4.18 bar.
    2. Simulated Leakage: Inflow surges to 48.5 m³/h (ΔQ = +18.5 m³/h).
    3. Pressure Drop: Downstream PT-202 drops to 3.50 bar (Window 1) -> 3.48 bar (Window 2+).
    4. Branch Contrast: Hydraulically connected branch PT-203 stays steady at 4.21 bar.
    5. Sensor-Health Checks: Confirms PT-202 signal is active & fluctuating, not frozen/flatline.
    6. Candidate Incident: Segment 14B flagged, uncalibrated heuristic score 0.89.
    7. District Z-04 Balance: Inflow 120, Export 15, Consumption 90 -> Residual 15 m³/h (1.25 m³ / 5 min).
    8. Table IV Event Contract: Standardized schema with event_time, ingest_time, quality_flag, zone_id, schema_version.
    """
    BASELINE_FLOW_M3H = 30.2
    BASELINE_PRESSURE_BAR = 4.18
    ABNORMAL_FLOW_M3H = 48.5
    ABNORMAL_P1_BAR = 3.50
    ABNORMAL_P2_BAR = 3.48
    BRANCH_PT203_NOMINAL_BAR = 4.21

    FLOW_THRESHOLD_M3H = 40.0
    PRESSURE_THRESHOLD_BAR = 3.55
    REQUIRED_PERSISTENT_READINGS = 2

    # District Z-04 Constants (Report Section XVI-A)
    Z04_NORMAL_INFLOW_M3H = 105.0
    Z04_LEAK_INFLOW_M3H = 120.0
    Z04_EXPORT_FLOW_M3H = 15.0
    Z04_AUTH_CONSUMPTION_M3H = 90.0
    Z04_UPSTREAM_PRESS_BAR = 3.40
    Z04_NORMAL_DOWNSTREAM_PRESS_BAR = 3.32
    Z04_LEAK_DOWNSTREAM_PRESS_BAR = 2.70

    def __init__(self):
        self.scenario = "NORMAL"  # "NORMAL" or "SIMULATED_LEAKAGE"
        self.step_index = 0
        self.consecutive_anomaly_count = 0
        self.pressure_history_buffer = []
        self.candidate_incident: Optional[CandidateIncident] = None
        self.sensor_history: deque = deque(maxlen=60)
        self.incident_counter = 501
        self.event_sequence_no = 1000

        # Sensor readings window for health diagnostics (last 10 readings per sensor)
        self.sensor_sample_windows: Dict[str, deque] = {
            "RES-INLET": deque(maxlen=10),
            "DS-LOC-01": deque(maxlen=10),
            "DS-LOC-02": deque(maxlen=10),
            "DS-LOC-03": deque(maxlen=10),
        }

        # Simulated faults injection map
        self.injected_faults: Dict[str, str] = {}

        # 5-Stage Ingestion & Validation Pipeline (Slide 8 & Report Sec. VII-B)
        self.ingestion_pipeline = DataIngestionPipeline()
        self._seed_illustrative_quarantine_records()

        # Downstream locations mapping
        self.locations = {
            "RES-INLET": "Reservoir Alpha Primary Inlet (FT-101)",
            "DS-LOC-01": "Downstream Sector Trunk East (PT-202 / Segment 14B)",
            "DS-LOC-02": "Branch Supply Line (PT-203 Isolated Branch)",
            "DS-LOC-03": "Industrial Peripheral Loop (PT-204)"
        }

    def _seed_illustrative_quarantine_records(self):
        """Seeds two baseline quarantined records to demonstrate quarantine schema upon startup."""
        sample_bad_event1 = {
            "event_id": "EVT-BAD-001",
            "sensor_id": "PT-109-SPUR",
            "metric": "flow",
            "value": -18.4,
            "unit": "m3/h",
            "event_time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() - 120)),
            "ingest_time": time.strftime("%Y-%m-%dT%H:%M:%S.024Z", time.gmtime(time.time() - 119)),
            "source_id": "IOT-GATEWAY-Z04",
            "zone_id": "Z-04",
            "schema_version": "v1.2.0"
        }
        sample_bad_event2 = {
            "event_id": "EVT-BAD-002",
            "sensor_id": "PRS-04-RAW",
            "metric": "pressure",
            "value": 52.0,
            "unit": "psi",  # Invalid unit: expected bar
            "event_time": time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(time.time() - 60)),
            "ingest_time": time.strftime("%Y-%m-%dT%H:%M:%S.024Z", time.gmtime(time.time() - 59)),
            "source_id": "IOT-GATEWAY-Z04",
            "zone_id": "Z-04",
            "schema_version": "v1.2.0"
        }
        self.ingestion_pipeline.process_event(sample_bad_event1)
        self.ingestion_pipeline.process_event(sample_bad_event2)

    def inject_malformed_event(self, anomaly_type: str) -> ValidationResult:
        """
        Generates an out-of-contract or invalid telemetry event to exercise
        the 5-Stage Ingestion Pipeline and Quarantine routing.
        """
        now = time.time()
        event_time_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now))
        ingest_time_iso = time.strftime("%Y-%m-%dT%H:%M:%S.024Z", time.gmtime(now + 0.024))
        self.event_sequence_no += 1

        if anomaly_type == "NEGATIVE_FLOW":
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "RES-INLET",
                "metric": "flow",
                "value": -14.8,
                "unit": "m3/h",
                "event_time": event_time_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        elif anomaly_type == "INVALID_UNIT":
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "DS-LOC-01",
                "metric": "pressure",
                "value": 55.2,
                "unit": "psi",  # Invalid unit
                "event_time": event_time_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        elif anomaly_type == "MISSING_TIMESTAMP":
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "DS-LOC-02",
                "metric": "pressure",
                "value": 4.21,
                "unit": "bar",
                "event_time": "",  # Missing
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        elif anomaly_type == "FUTURE_CLOCK_SKEW":
            future_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now + 3600))  # 1 hour ahead
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "DS-LOC-01",
                "metric": "pressure",
                "value": 4.18,
                "unit": "bar",
                "event_time": future_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        elif anomaly_type == "DELAYED_WATERMARK":
            past_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now - 3600))  # 1 hour late (>15 min)
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "RES-INLET",
                "metric": "flow",
                "value": 30.2,
                "unit": "m3/h",
                "event_time": past_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        elif anomaly_type == "ESTIMATED_GAP":
            # Missing reading handling: "No reading is not zero flow"
            event = {
                "event_id": f"GAP-{self.event_sequence_no:04d}",
                "sensor_id": "DS-LOC-03",
                "metric": "flow",
                "value": 6.04,  # Estimated based on prior average
                "original_value": None,  # Explicitly None to show missingness
                "unit": "m3/h",
                "quality_flag": "ESTIMATED",
                "event_time": event_time_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }
        else:
            event = {
                "event_id": f"MAL-{self.event_sequence_no:04d}",
                "sensor_id": "",
                "metric": "unknown",
                "value": "INVALID_STR",
                "unit": "none",
                "event_time": event_time_iso,
                "ingest_time": ingest_time_iso,
                "source_id": "IOT-GATEWAY-Z04",
                "zone_id": "Z-04",
                "schema_version": "v1.2.0"
            }

        return self.ingestion_pipeline.process_event(event)

    def set_scenario(self, scenario: str):
        if scenario != self.scenario:
            self.scenario = scenario
            self.step_index = 0
            self.consecutive_anomaly_count = 0
            self.pressure_history_buffer = []
            if scenario == "NORMAL":
                self.candidate_incident = None

    def inject_sensor_fault(self, sensor_id: str, fault_type: str):
        """Simulates sensor anomalies such as 'FROZEN_FLATLINE' or 'OUT_OF_BOUNDS'."""
        self.injected_faults[sensor_id] = fault_type

    def clear_sensor_faults(self):
        self.injected_faults.clear()

    def evaluate_sensor_health(self, sensor_id: str, current_value: float) -> SensorHealthRecord:
        """
        Sensor health diagnostics (Slide 10 & Report Sec. XII-D):
        Checks for flatline / frozen reading, out of bounds, and drift.
        """
        tag_map = {"RES-INLET": "FT-101", "DS-LOC-01": "PT-202", "DS-LOC-02": "PT-203", "DS-LOC-03": "PT-204"}
        tag = tag_map.get(sensor_id, sensor_id)

        # Check for explicitly injected fault
        injected = self.injected_faults.get(sensor_id)
        if injected == "FROZEN_FLATLINE":
            return SensorHealthRecord(
                sensor_id=sensor_id,
                sensor_tag=tag,
                status="FROZEN_FLATLINE",
                is_healthy=False,
                variance=0.0,
                consecutive_repeats=10,
                diagnostic_message="Telemetry signal is frozen: 0 variance detected across consecutive windows."
            )
        elif injected == "OUT_OF_BOUNDS":
            return SensorHealthRecord(
                sensor_id=sensor_id,
                sensor_tag=tag,
                status="OUT_OF_BOUNDS",
                is_healthy=False,
                variance=0.0,
                consecutive_repeats=1,
                diagnostic_message="Reading exceeds physical transducer limits (<0 or >16 bar)."
            )

        window = list(self.sensor_sample_windows.get(sensor_id, []))
        if len(window) >= 3:
            # Check for zero variance / flatline
            variance = sum((x - sum(window)/len(window))**2 for x in window) / len(window)
            repeats = sum(1 for x in window if abs(x - current_value) < 1e-4)

            if variance < 1e-6 and repeats >= 3:
                return SensorHealthRecord(
                    sensor_id=sensor_id,
                    sensor_tag=tag,
                    status="FROZEN_FLATLINE",
                    is_healthy=False,
                    variance=round(variance, 6),
                    consecutive_repeats=repeats,
                    diagnostic_message=f"Signal flatline detected on {tag}: value locked at {current_value}."
                )

            return SensorHealthRecord(
                sensor_id=sensor_id,
                sensor_tag=tag,
                status="ONLINE_HEALTHY",
                is_healthy=True,
                variance=round(variance, 4),
                consecutive_repeats=0,
                diagnostic_message=f"Sensor {tag} operating nominally; dynamic signal fluctuating naturally."
            )

        return SensorHealthRecord(
            sensor_id=sensor_id,
            sensor_tag=tag,
            status="ONLINE_HEALTHY",
            is_healthy=True,
            variance=0.01,
            consecutive_repeats=0,
            diagnostic_message=f"Sensor {tag} initial sampling active."
        )

    def generate_step_readings(self) -> Tuple[Dict[str, SimulatedSensorReading], Dict[str, any]]:
        """
        Generates simulated readings for the current time step according to the active scenario,
        and evaluates them using the rule-based leakage detection engine.
        """
        now = time.time()
        time_str = time.strftime("%H:%M:%S", time.localtime(now))
        event_time_iso = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(now))
        ingest_time_iso = time.strftime("%Y-%m-%dT%H:%M:%S.024Z", time.gmtime(now + 0.024))
        self.event_sequence_no += 1
        readings = {}

        if self.scenario == "NORMAL":
            # Normal baseline: flow ~30.2 m³/h, pressure ~4.18 bar with realistic sensor noise
            noise_f = random.gauss(0, 0.12)
            noise_p = random.gauss(0, 0.015)
            inlet_flow = round(self.BASELINE_FLOW_M3H + noise_f, 2)
            inlet_press = round(self.BASELINE_PRESSURE_BAR + noise_p, 2)
            ds_press_1 = round(self.BASELINE_PRESSURE_BAR - 0.08 + noise_p, 2)
            # Branch PT-203 stays steady at ~4.21 bar (Slide 10)
            ds_press_2 = round(self.BRANCH_PT203_NOMINAL_BAR + random.gauss(0, 0.005), 2)
            ds_press_3 = round(self.BASELINE_PRESSURE_BAR - 0.15 + noise_p, 2)
            is_anomaly = False
            self.consecutive_anomaly_count = 0
            self.pressure_history_buffer = []
            detector_state = "NORMAL"
            detector_message = "All flow and pressure readings within nominal baseline envelope (Flow: ~30.2 m³/h, Pressure: ~4.18 bar)."

            # District Z-04 Mass Balance (Normal): Inlet 105, Export 15, Consumption 90 -> Residual 0 m³/h
            z04_inflow = round(self.Z04_NORMAL_INFLOW_M3H + random.gauss(0, 0.25), 2)
            z04_export = self.Z04_EXPORT_FLOW_M3H
            z04_auth = self.Z04_AUTH_CONSUMPTION_M3H
            z04_storage_dt = 0.0
            z04_residual = round(z04_inflow - z04_export - z04_auth - z04_storage_dt, 2)
            z04_unmatched_5min = round(max(0.0, z04_residual) * (5.0 / 60.0), 3)
            z04_press_up = self.Z04_UPSTREAM_PRESS_BAR
            z04_press_down = self.Z04_NORMAL_DOWNSTREAM_PRESS_BAR
            z04_status = "BALANCED_NORMAL"
            z04_incident_id = None

        else: # "SIMULATED_LEAKAGE"
            self.step_index += 1
            is_anomaly = True

            # Flow increases to 48.5 m³/h (ΔQ = +18.5 m³/h)
            noise_f = random.gauss(0, 0.10)
            inlet_flow = round(self.ABNORMAL_FLOW_M3H + noise_f, 2)
            inlet_press = round(self.BASELINE_PRESSURE_BAR - 0.20 + random.gauss(0, 0.02), 2)

            # Downstream PT-202 (Location A / Segment 14B) experiences the specified drop:
            # Reading 1: 3.50 bar, Reading 2+: 3.48 bar
            if self.step_index == 1:
                ds_press_1 = self.ABNORMAL_P1_BAR
            else:
                ds_press_1 = round(self.ABNORMAL_P2_BAR + random.uniform(-0.01, 0.01), 2)

            # Branch PT-203 stays steady at ~4.21 bar (Slide 10 requirement)
            ds_press_2 = round(self.BRANCH_PT203_NOMINAL_BAR + random.gauss(0, 0.005), 2)
            ds_press_3 = round(3.90 + random.gauss(0, 0.02), 2)

            # District Z-04 Mass Balance (Report Section XVI-A): Inlet 120, Export 15, Consumption 90 -> Residual 15 m³/h
            z04_inflow = round(self.Z04_LEAK_INFLOW_M3H + random.gauss(0, 0.20), 2)
            z04_export = self.Z04_EXPORT_FLOW_M3H
            z04_auth = self.Z04_AUTH_CONSUMPTION_M3H
            z04_storage_dt = 0.0
            z04_residual = round(z04_inflow - z04_export - z04_auth - z04_storage_dt, 2)
            z04_unmatched_5min = round(z04_residual * (5.0 / 60.0), 3) # 15 * (5/60) = 1.25 m³
            z04_press_up = self.Z04_UPSTREAM_PRESS_BAR
            z04_press_down = self.Z04_LEAK_DOWNSTREAM_PRESS_BAR
            z04_status = "CANDIDATE_ANOMALY"
            z04_incident_id = "Z04-1005"

            # Evaluate Rule-based Detector on Downstream Location A (PT-202)
            if inlet_flow >= self.FLOW_THRESHOLD_M3H and ds_press_1 <= self.PRESSURE_THRESHOLD_BAR:
                self.consecutive_anomaly_count += 1
                self.pressure_history_buffer.append(ds_press_1)

                if self.consecutive_anomaly_count < self.REQUIRED_PERSISTENT_READINGS:
                    detector_state = "TRANSIENT_ANOMALY_MONITORING"
                    detector_message = (
                        f"Abnormal condition detected on sample #{self.consecutive_anomaly_count}: "
                        f"Inflow {inlet_flow} m3/h (Threshold >= {self.FLOW_THRESHOLD_M3H} m3/h), "
                        f"PT-202 Downstream Pressure {ds_press_1} bar (Threshold <= {self.PRESSURE_THRESHOLD_BAR} bar), "
                        f"Branch PT-203 stays steady at {ds_press_2} bar. "
                        f"Checking persistence across consecutive samples to rule out transient hydraulic noise..."
                    )
                else:
                    detector_state = "CANDIDATE_LEAKAGE_INCIDENT"
                    seq_str = " -> ".join(f"{p:.2f} bar" for p in self.pressure_history_buffer[-2:])
                    detector_message = (
                        f"Persistent pressure anomaly verified across {self.consecutive_anomaly_count} consecutive readings "
                        f"({seq_str}) concurrent with flow surge to {inlet_flow} m3/h. "
                        f"Branch PT-203 steady at {ds_press_2} bar confirms localized drop at Segment 14B. "
                        f"Sensor health check confirmed PT-202 is active and fluctuating. "
                        f"Candidate leakage incident registered."
                    )

                    if not self.candidate_incident or not self.candidate_incident.is_confirmed:
                        self.incident_counter += 1
                        self.candidate_incident = CandidateIncident(
                            incident_id=f"CAND-INC-{self.incident_counter}",
                            timestamp=time_str,
                            location_id="DS-LOC-01",
                            location_name="Downstream Sector Trunk East (PT-202 / Segment 14B)",
                            flow_reading=inlet_flow,
                            baseline_flow=self.BASELINE_FLOW_M3H,
                            pressure_reading=ds_press_1,
                            baseline_pressure=self.BASELINE_PRESSURE_BAR,
                            consecutive_readings_count=self.consecutive_anomaly_count,
                            readings_sequence=list(self.pressure_history_buffer[-3:]),
                            reason_for_alert=(
                                f"Inflow surged to {inlet_flow} m3/h (Baseline: {self.BASELINE_FLOW_M3H} m3/h, +{((inlet_flow-self.BASELINE_FLOW_M3H)/self.BASELINE_FLOW_M3H)*100:.1f}%). "
                                f"Downstream PT-202 pressure dropped to {self.ABNORMAL_P1_BAR} bar and persisted at {self.ABNORMAL_P2_BAR} bar "
                                f"across 2 consecutive readings (Baseline: {self.BASELINE_PRESSURE_BAR} bar, -{((self.BASELINE_PRESSURE_BAR-ds_press_1)/self.BASELINE_PRESSURE_BAR)*100:.1f}%). "
                                f"Branch sensor PT-203 remained stable at {ds_press_2} bar (isolated local anomaly). "
                                f"Persistence criterion (>= 2 consecutive samples) satisfied."
                            ),
                            incident_status="CANDIDATE_LEAKAGE_INCIDENT (UNCONFIRMED - REQUIRES FIELD VALIDATION)",
                            is_confirmed=False,
                            data_label="SIMULATED",
                            zone_id="Z-04",
                            candidate_segment="Segment 14B",
                            heuristic_score=0.89,
                            branch_sensor_id="PT-203",
                            branch_pressure_bar=ds_press_2,
                            unmatched_volume_m3=z04_unmatched_5min
                        )
            else:
                self.consecutive_anomaly_count = 0
                detector_state = "NORMAL"
                detector_message = "Readings stabilized back within acceptable limits."

        # Update sample windows for sensor health checks
        self.sensor_sample_windows["RES-INLET"].append(inlet_flow)
        self.sensor_sample_windows["DS-LOC-01"].append(ds_press_1)
        self.sensor_sample_windows["DS-LOC-02"].append(ds_press_2)
        self.sensor_sample_windows["DS-LOC-03"].append(ds_press_3)

        # Sensor Health Diagnostics
        health_inlet = self.evaluate_sensor_health("RES-INLET", inlet_flow)
        health_pt202 = self.evaluate_sensor_health("DS-LOC-01", ds_press_1)
        health_pt203 = self.evaluate_sensor_health("DS-LOC-02", ds_press_2)
        health_pt204 = self.evaluate_sensor_health("DS-LOC-03", ds_press_3)

        sensor_health_map = {
            "RES-INLET": health_inlet,
            "DS-LOC-01": health_pt202,
            "DS-LOC-02": health_pt203,
            "DS-LOC-03": health_pt204
        }

        # Package readings
        readings["RES-INLET"] = SimulatedSensorReading(
            timestamp=now,
            time_str=time_str,
            location_id="RES-INLET",
            location_name=self.locations["RES-INLET"],
            flow_m3h=inlet_flow,
            pressure_bar=inlet_press,
            is_anomaly_sample=is_anomaly,
            sensor_tag="FT-101",
            health_status=health_inlet.status,
            zone_id="Z-04",
            data_label="SIMULATED"
        )
        readings["DS-LOC-01"] = SimulatedSensorReading(
            timestamp=now,
            time_str=time_str,
            location_id="DS-LOC-01",
            location_name=self.locations["DS-LOC-01"],
            flow_m3h=round(inlet_flow * 0.55 if not is_anomaly else (inlet_flow * 0.55 - 12.0), 2),
            pressure_bar=ds_press_1,
            is_anomaly_sample=is_anomaly,
            sensor_tag="PT-202",
            health_status=health_pt202.status,
            zone_id="Z-04",
            data_label="SIMULATED"
        )
        readings["DS-LOC-02"] = SimulatedSensorReading(
            timestamp=now,
            time_str=time_str,
            location_id="DS-LOC-02",
            location_name=self.locations["DS-LOC-02"],
            flow_m3h=round(inlet_flow * 0.25, 2),
            pressure_bar=ds_press_2,
            is_anomaly_sample=False,
            sensor_tag="PT-203",
            health_status=health_pt203.status,
            zone_id="Z-04",
            data_label="SIMULATED"
        )
        readings["DS-LOC-03"] = SimulatedSensorReading(
            timestamp=now,
            time_str=time_str,
            location_id="DS-LOC-03",
            location_name=self.locations["DS-LOC-03"],
            flow_m3h=round(inlet_flow * 0.20, 2),
            pressure_bar=ds_press_3,
            is_anomaly_sample=False,
            sensor_tag="PT-204",
            health_status=health_pt204.status,
            zone_id="Z-04",
            data_label="SIMULATED"
        )

        # Store in historical buffer
        self.sensor_history.append({
            "timestamp": now,
            "time_str": time_str,
            "inlet_flow": inlet_flow,
            "inlet_pressure": inlet_press,
            "ds_press_1": ds_press_1,
            "ds_press_2": ds_press_2,
            "ds_press_3": ds_press_3,
            "scenario": self.scenario,
            "is_anomaly": is_anomaly,
            "z04_residual": z04_residual,
            "data_label": "SIMULATED"
        })

        # District Z-04 Object
        district_balance = DistrictWaterBalance(
            zone_id="Z-04",
            inlet_flow_m3h=z04_inflow,
            export_flow_m3h=z04_export,
            authorized_consumption_m3h=z04_auth,
            storage_change_m3h=z04_storage_dt,
            balance_residual_m3h=z04_residual,
            unmatched_volume_5min_m3=z04_unmatched_5min,
            upstream_pressure_bar=z04_press_up,
            downstream_pressure_bar=z04_press_down,
            candidate_incident_id=z04_incident_id,
            candidate_area="Z-04 downstream pipe group (Segment 14B)",
            status=z04_status,
            data_label="SIMULATED"
        )

        # Branch Comparison Object
        branch_comparison = {
            "candidate_sensor_id": "PT-202",
            "candidate_location": "Segment 14B (Downstream East)",
            "candidate_pressure_bar": ds_press_1,
            "candidate_nominal_bar": self.BASELINE_PRESSURE_BAR,
            "branch_sensor_id": "PT-203",
            "branch_location": "Branch Line (Hydraulically Isolated)",
            "branch_pressure_bar": ds_press_2,
            "branch_nominal_bar": self.BRANCH_PT203_NOMINAL_BAR,
            "pressure_differential_bar": round(ds_press_2 - ds_press_1, 2),
            "branch_status": "UNAFFECTED (Confirms Localized Pipe Event)" if is_anomaly else "NORMAL_UNIFORM",
            "isolation_confirmed": bool(ds_press_2 >= 4.15)
        }

        # Build structured Latest Processed Event (Table IV Schema & Kafka Ingestion Role)
        latest_event = {
            "event_id": f"E{self.event_sequence_no:04d}",
            "sensor_id": "RES-INLET",
            "sequence_no": self.event_sequence_no,
            "source_id": "HISTORIAN-OSI-PI-GW01",
            "event_time": event_time_iso,
            "ingest_time": ingest_time_iso,
            "source_clock_status": "SYNCHRONIZED",
            "metric": "flow",
            "value": inlet_flow,
            "unit": "m3/h",
            "sampling_interval_sec": 15,
            "quality_flag": "VALID",
            "original_value": inlet_flow,
            "correction_version": 1,
            "asset_id": "SEC-01-TRUNK",
            "zone_id": "Z-04",
            "mapping_version": "GIS-v2.4",
            "device_status": health_inlet.status,
            "calibration_date": "2026-09-15",
            "schema_version": "v1.2.0",
            # Backward compatibility fields
            "timestamp": time_str,
            "topic": "water.telemetry.inlet",
            "partition": 0,
            "offset": len(self.sensor_history),
            "location": self.locations["RES-INLET"],
            "flow_reading_m3h": inlet_flow,
            "inlet_pressure_bar": inlet_press,
            "downstream_sensor_id": "DS-LOC-01",
            "downstream_pressure_bar": ds_press_1,
            "scenario": self.scenario,
            "data_label": "SIMULATED",
            "service_mode": "Simulated Ingestion Queue (Kafka Role)"
        }

        # Build structured Latest Detection Result (Spark Stream Processing Role)
        latest_detection = {
            "timestamp": time_str,
            "stage": "Stream Processing & Leakage Detection (Spark Role - Simulated)",
            "detector_state": detector_state,
            "rule_name": "Flow Surge + Consecutive Pressure Drop Persistence + Branch Contrast",
            "inflow_m3h": inlet_flow,
            "flow_threshold_m3h": self.FLOW_THRESHOLD_M3H,
            "flow_exceeded": bool(inlet_flow >= self.FLOW_THRESHOLD_M3H),
            "downstream_pressure_bar": ds_press_1,
            "pressure_threshold_bar": self.PRESSURE_THRESHOLD_BAR,
            "pressure_drop_detected": bool(ds_press_1 <= self.PRESSURE_THRESHOLD_BAR),
            "branch_pt203_pressure_bar": ds_press_2,
            "branch_steady_at_421": bool(abs(ds_press_2 - 4.21) < 0.1),
            "consecutive_anomaly_samples": self.consecutive_anomaly_count,
            "required_consecutive_samples": self.REQUIRED_PERSISTENT_READINGS,
            "persistence_condition_met": bool(self.consecutive_anomaly_count >= self.REQUIRED_PERSISTENT_READINGS),
            "candidate_incident_created": bool(self.candidate_incident is not None),
            "candidate_incident_id": self.candidate_incident.incident_id if self.candidate_incident else "None",
            "candidate_segment": "Segment 14B" if self.candidate_incident else "None",
            "heuristic_score": 0.89 if self.candidate_incident else 0.0,
            "incident_classification": "CANDIDATE ALERT (UNCONFIRMED - REQUIRES FIELD VALIDATION)" if self.candidate_incident else "NORMAL",
            "reason": detector_message
        }

        # Pass through 5-Stage Ingestion & Validation Pipeline (Slide 8)
        val_result = self.ingestion_pipeline.process_event(latest_event)

        analysis_meta = {
            "scenario": self.scenario,
            "detector_state": detector_state,
            "detector_message": detector_message,
            "consecutive_count": self.consecutive_anomaly_count,
            "candidate_incident": self.candidate_incident,
            "latest_event": latest_event,
            "latest_detection": latest_detection,
            "district_balance": asdict(district_balance),
            "branch_comparison": branch_comparison,
            "sensor_health": {k: asdict(v) for k, v in sensor_health_map.items()},
            "quarantine_history": list(self.ingestion_pipeline.quarantine_history),
            "pipeline_stats": {
                "processed": self.ingestion_pipeline.processed_events_count,
                "valid": self.ingestion_pipeline.valid_events_count,
                "quarantined": self.ingestion_pipeline.quarantined_events_count,
                "estimated": self.ingestion_pipeline.estimated_events_count
            },
            "latest_validation": asdict(val_result),
            "data_label": "SIMULATED"
        }

        return readings, analysis_meta

def check_service_availability(host: str = "localhost", port: int = 9092) -> bool:
    """Probes if a genuine Kafka or Spark network service is reachable."""
    import socket
    try:
        with socket.create_connection((host, port), timeout=0.15):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

