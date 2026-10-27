"""
Apache Spark Real-Time Streaming Detection Engine
Simulates Spark Structured Streaming micro-batch sliding window analytics,
Mass Balance Conservation equations, hydraulic gradient anomaly scoring,
and automated leakage classification.
"""

import time
import math
from typing import Dict, List, Any, Optional

class SparkStreamingDetector:
    """
    Simulates Apache Spark Structured Streaming processing pipeline.
    Executes windowed mass-balance verification and hydraulic anomaly detection.
    """
    def __init__(self, window_size_sec: int = 15, mass_balance_threshold_pct: float = 4.0):
        self.window_size_sec = window_size_sec
        self.threshold_pct = mass_balance_threshold_pct
        self.job_name = "SparkStructuredStreaming_WaterLeakageAnalysis"
        self.spark_master = "spark://master.watergrid.local:7077"
        self.active_alerts: List[Dict[str, Any]] = []
        self.incident_counter = 1000

    def process_micro_batch(self, segment, latest_reading: Dict[str, Any], history_readings: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Executes Spark Streaming micro-batch job on the current sliding window.
        """
        if segment.valve_status == "ISOLATED":
            # If isolated, suppress false leak alerts
            return None

        inlet_flow = latest_reading.get("inlet_flow", 0.0)
        outlet_flow = latest_reading.get("outlet_flow", 0.0)
        inlet_press = latest_reading.get("inlet_pressure", 0.0)
        outlet_press = latest_reading.get("outlet_pressure", 0.0)

        # Spark Windowed Aggregations (Sliding Average over window)
        relevant_history = [
            r for r in history_readings[-10:]
            if r.get("segment_id") == segment.segment_id
        ]

        if relevant_history:
            avg_inlet = sum(r.get("inlet_flow", 0.0) for r in relevant_history) / len(relevant_history)
            avg_outlet = sum(r.get("outlet_flow", 0.0) for r in relevant_history) / len(relevant_history)
        else:
            avg_inlet = inlet_flow
            avg_outlet = outlet_flow

        # Mass Balance Equation: ΔQ = Q_in - Q_out
        delta_q = avg_inlet - avg_outlet
        discrepancy_pct = (delta_q / max(1.0, avg_inlet)) * 100.0

        # Hydraulic Pressure Gradient
        normal_frictional_drop = 0.12 * segment.length_km
        actual_pressure_drop = inlet_press - outlet_press
        abnormal_pressure_drop = max(0.0, actual_pressure_drop - normal_frictional_drop)

        # Leak criteria
        is_leak = False
        severity = "NORMAL"
        confidence = 0.0

        if delta_q > 150.0 or discrepancy_pct > 25.0 or abnormal_pressure_drop > 1.2:
            is_leak = True
            severity = "CRITICAL_BURST"
            confidence = min(99.4, 85.0 + discrepancy_pct * 0.4)
        elif delta_q > 40.0 or discrepancy_pct > 7.5 or abnormal_pressure_drop > 0.45:
            is_leak = True
            severity = "WARNING_LEAK"
            confidence = min(95.0, 72.0 + discrepancy_pct * 0.5)
        elif delta_q > 12.0 or discrepancy_pct > 3.0 or abnormal_pressure_drop > 0.2:
            is_leak = True
            severity = "MINOR_SEEPAGE"
            confidence = min(88.0, 60.0 + discrepancy_pct * 0.7)

        if is_leak:
            # Estimate leak distance along pipeline using hydraulic gradient
            # Ratio of pressure deviation from upstream vs downstream
            loc_ratio = min(0.95, max(0.05, 0.5 + (actual_pressure_drop - normal_frictional_drop) * 0.1))
            estimated_distance_km = round(segment.length_km * loc_ratio, 2)

            self.incident_counter += 1
            alert = {
                "incident_id": f"INC-{self.incident_counter}",
                "timestamp": time.strftime("%H:%M:%S"),
                "segment_id": segment.segment_id,
                "segment_name": segment.name,
                "severity": severity,
                "confidence_pct": round(confidence, 1),
                "delta_flow_lpm": round(delta_q, 2),
                "inlet_flow": round(inlet_flow, 1),
                "outlet_flow": round(outlet_flow, 1),
                "abnormal_pressure_drop_bar": round(abnormal_pressure_drop, 2),
                "estimated_distance_km": estimated_distance_km,
                "valve_id": segment.valve_id,
                "recommended_action": f"Emergency isolate {segment.valve_id} to halt water loss."
            }
            return alert

        return None
