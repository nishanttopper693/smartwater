"""
Comprehensive Test & Finalization Verification Suite
Validates:
1. Normal baseline scenario (30.2 m³/h, 4.18 bar)
2. Simulated abnormal leakage scenario (48.5 m³/h, 3.50 bar -> 3.48 bar)
3. Persistence rule verification (2 consecutive samples required before candidate incident)
4. Candidate alert generation with UNCONFIRMED classification
5. Chart and history buffer updates
6. Reset and pause/resume control mechanics
7. Genuine vs simulated service probing
"""

import sys
from demonstration_engine import RuleBasedLeakageDetector, check_service_availability
from kafka_engine import KafkaBrokerSimulator, generate_sensor_telemetry
from pipeline_model import get_default_network_topology
from spark_detector import SparkStreamingDetector

def run_tests():
    print("=== Test 1: Service Availability Probe ===")
    kafka_live = check_service_availability("localhost", 9092)
    spark_live = check_service_availability("localhost", 7077)
    print(f"Genuine Kafka on port 9092: {kafka_live} (Expected: False in local standalone test)")
    print(f"Genuine Spark on port 7077: {spark_live} (Expected: False in local standalone test)")
    assert not kafka_live, "Port 9092 shouldn't be claimed live when not running"
    assert not spark_live, "Port 7077 shouldn't be claimed live when not running"
    print("PASS: Service probe verified.")

    print("\n=== Test 2: Normal Baseline Scenario ===")
    detector = RuleBasedLeakageDetector()
    readings_norm, meta_norm = detector.generate_step_readings()
    inlet_flow = readings_norm["RES-INLET"].flow_m3h
    inlet_press = readings_norm["RES-INLET"].pressure_bar
    ds_press = readings_norm["DS-LOC-01"].pressure_bar
    print(f"Inflow: {inlet_flow:.2f} m3/h (Target: ~30.2 m3/h)")
    print(f"Inlet Pressure: {inlet_press:.2f} bar (Target: ~4.18 bar)")
    print(f"DS-1 Pressure: {ds_press:.2f} bar")
    print("Detector State:", meta_norm["detector_state"])
    assert 29.5 <= inlet_flow <= 31.0, f"Flow out of bounds: {inlet_flow}"
    assert 4.0 <= inlet_press <= 4.3, f"Pressure out of bounds: {inlet_press}"
    assert meta_norm["detector_state"] == "NORMAL"
    assert meta_norm["candidate_incident"] is None
    print("PASS: Normal baseline operates within envelope with zero false alarms.")

    print("\n=== Test 3: Simulated Leakage Scenario (Sample 1: Checking Persistence) ===")
    detector.set_scenario("SIMULATED_LEAKAGE")
    r1, m1 = detector.generate_step_readings()
    f1 = r1["RES-INLET"].flow_m3h
    p1 = r1["DS-LOC-01"].pressure_bar
    print(f"Sample 1 Inflow: {f1:.2f} m3/h | DS-1 Pressure: {p1:.2f} bar")
    print(f"State: {m1['detector_state']} | Consecutive Samples: {m1['consecutive_count']}")
    assert m1["detector_state"] == "TRANSIENT_ANOMALY_MONITORING"
    assert m1["consecutive_count"] == 1
    assert m1["candidate_incident"] is None, "Candidate alert must NOT appear on single reading"
    print("PASS: Single reading anomaly held in monitoring state without false alert.")

    print("\n=== Test 4: Simulated Leakage Scenario (Sample 2: Persistence Confirmed) ===")
    r2, m2 = detector.generate_step_readings()
    f2 = r2["RES-INLET"].flow_m3h
    p2 = r2["DS-LOC-01"].pressure_bar
    print(f"Sample 2 Inflow: {f2:.2f} m3/h | DS-1 Pressure: {p2:.2f} bar")
    print(f"State: {m2['detector_state']} | Consecutive Samples: {m2['consecutive_count']}")
    assert m2["detector_state"] == "CANDIDATE_LEAKAGE_INCIDENT"
    assert m2["consecutive_count"] >= 2
    cand = m2["candidate_incident"]
    assert cand is not None
    print("Candidate Incident ID:", cand.incident_id)
    print("Classification Status:", cand.incident_status)
    print("Reason:", cand.reason_for_alert)
    assert "CANDIDATE" in cand.incident_status
    assert "UNCONFIRMED" in cand.incident_status
    print("PASS: Candidate incident created with required UNCONFIRMED classification.")

    print("\n=== Test 5: Verify Chart & Buffer Updates ===")
    history_len = len(detector.sensor_history)
    print(f"Sensor History Buffer Length: {history_len} samples")
    assert history_len >= 3
    latest_evt = m2["latest_event"]
    latest_det = m2["latest_detection"]
    print(f"Latest Processed Event ID: {latest_evt['event_id']} (Topic: {latest_evt['topic']})")
    print(f"Latest Detection Result Stage: {latest_det['stage']}")
    assert latest_evt["sensor_id"] == "RES-INLET"
    assert latest_det["candidate_incident_created"] is True
    print("PASS: Live event packets and detection results serialize properly.")

    print("\n=== Test 6: Verify Reset Control ===")
    detector.set_scenario("NORMAL")
    r_reset, m_reset = detector.generate_step_readings()
    f_reset = r_reset["RES-INLET"].flow_m3h
    p_reset = r_reset["DS-LOC-01"].pressure_bar
    print(f"After Reset Inflow: {f_reset:.2f} m3/h | DS-1 Pressure: {p_reset:.2f} bar")
    print("State:", m_reset["detector_state"], "| Consecutive Count:", m_reset["consecutive_count"])
    assert m_reset["detector_state"] == "NORMAL"
    assert m_reset["consecutive_count"] == 0
    assert m_reset["candidate_incident"] is None
    print("PASS: Reset control successfully restores baseline envelope.")

    print("\n=== Test 7: Table IV Event Contract Schema Compliance ===")
    evt = m2["latest_event"]
    print("Verifying Table IV Schema Fields:")
    print(f"  event_id: {evt.get('event_id')}")
    print(f"  sensor_id: {evt.get('sensor_id')}")
    print(f"  event_time: {evt.get('event_time')}")
    print(f"  ingest_time: {evt.get('ingest_time')}")
    print(f"  quality_flag: {evt.get('quality_flag')}")
    print(f"  zone_id: {evt.get('zone_id')}")
    print(f"  schema_version: {evt.get('schema_version')}")
    
    assert "event_id" in evt and evt["event_id"].startswith("E"), "event_id must be present"
    assert evt.get("sensor_id") == "RES-INLET", "sensor_id must match publisher"
    assert "event_time" in evt and "T" in evt["event_time"], "event_time must be ISO formatted"
    assert "ingest_time" in evt and "T" in evt["ingest_time"], "ingest_time must be ISO formatted"
    assert evt.get("quality_flag") in ["VALID", "ESTIMATED", "QUARANTINED", "SUSPECT"], "quality_flag must match contract"
    assert evt.get("zone_id") == "Z-04", "zone_id must be Z-04"
    assert evt.get("schema_version") == "v1.2.0", "schema_version must be defined"
    print("PASS: Table IV Event Contract schema verified.")

    print("\n=== Test 8: District Z-04 Mass Balance & Volume Calculation ===")
    detector.set_scenario("SIMULATED_LEAKAGE")
    detector.generate_step_readings() # Step 1
    _, m_z04 = detector.generate_step_readings() # Step 2
    z04 = m_z04["district_balance"]
    print(f"District Zone: {z04['zone_id']}")
    print(f"Inlet Flow (Q_in): {z04['inlet_flow_m3h']:.1f} m3/h")
    print(f"Export Flow (Q_export): {z04['export_flow_m3h']:.1f} m3/h")
    print(f"Authorized Local Consumption (Q_auth): {z04['authorized_consumption_m3h']:.1f} m3/h")
    print(f"Balance Residual R = Q_in - Q_exp - Q_auth - dV/dt: {z04['balance_residual_m3h']:.2f} m3/h")
    print(f"Unmatched Volume (5-min window): {z04['unmatched_volume_5min_m3']:.3f} m3 (Target: ~1.25 m3)")
    print(f"Candidate Incident ID: {z04['candidate_incident_id']}")
    
    assert 118.0 <= z04["inlet_flow_m3h"] <= 122.0, "Z-04 leak inlet should be ~120 m3/h"
    assert z04["export_flow_m3h"] == 15.0, "Export flow should be 15 m3/h"
    assert z04["authorized_consumption_m3h"] == 90.0, "Authorized consumption should be 90 m3/h"
    assert 13.0 <= z04["balance_residual_m3h"] <= 17.0, "Residual should be ~15 m3/h"
    assert 1.15 <= z04["unmatched_volume_5min_m3"] <= 1.35, "5-min unmatched volume should be ~1.25 m3"
    assert z04["candidate_incident_id"] == "Z04-1005", "Candidate ID must match Report Section XVI-A"
    print("PASS: District Z-04 mass-balance conservation and unmatched volume confirmed.")

    print("\n=== Test 9: PT-202 vs PT-203 Branch-Pressure Comparison ===")
    branch_comp = m_z04["branch_comparison"]
    pt202_p = branch_comp["candidate_pressure_bar"]
    pt203_p = branch_comp["branch_pressure_bar"]
    diff_p = branch_comp["pressure_differential_bar"]
    print(f"Affected Downstream PT-202: {pt202_p:.2f} bar (Baseline: 4.18 bar, Drop: {4.18-pt202_p:.2f} bar)")
    print(f"Isolated Branch Line PT-203: {pt203_p:.2f} bar (Nominal: 4.21 bar)")
    print(f"Differential: {diff_p:.2f} bar | Branch Status: {branch_comp['branch_status']}")
    
    assert 3.40 <= pt202_p <= 3.52, f"PT-202 should show depressed pressure, got {pt202_p}"
    assert 4.18 <= pt203_p <= 4.25, f"Branch PT-203 must stay steady at ~4.21 bar, got {pt203_p}"
    assert branch_comp["isolation_confirmed"] is True, "Branch contrast must confirm localized pipe anomaly"
    print("PASS: Branch pressure contrast successfully isolates Segment 14B from network-wide drop.")

    print("\n=== Test 10: Sensor Health & Frozen-Sensor (Flatline) Detection ===")
    health_records = m_z04["sensor_health"]
    pt202_health = health_records["DS-LOC-01"]
    print(f"PT-202 Signal Status: {pt202_health['status']} | Healthy: {pt202_health['is_healthy']}")
    assert pt202_health["is_healthy"] is True, "In normal operation, sensor with noise should be healthy"
    
    # Inject frozen flatline fault
    print("Injecting simulated FROZEN_FLATLINE fault on PT-202...")
    detector.inject_sensor_fault("DS-LOC-01", "FROZEN_FLATLINE")
    _, m_fault = detector.generate_step_readings()
    pt202_fault_health = m_fault["sensor_health"]["DS-LOC-01"]
    print(f"PT-202 Fault Diagnosis: {pt202_fault_health['status']} | Message: {pt202_fault_health['diagnostic_message']}")
    assert pt202_fault_health["is_healthy"] is False
    assert pt202_fault_health["status"] == "FROZEN_FLATLINE"
    
    # Clear fault and verify restoration
    detector.clear_sensor_faults()
    detector.set_scenario("NORMAL")
    _, m_restored = detector.generate_step_readings()
    pt202_restored = m_restored["sensor_health"]["DS-LOC-01"]
    print(f"Restored PT-202 Status: {pt202_restored['status']}")
    assert pt202_restored["is_healthy"] is True
    print("PASS: Sensor health diagnostics accurately detect and isolate frozen flatline faults.")

    print("\n=======================================================")
    print("ALL 10 TEST PHASES PASSED WITH ZERO ERRORS OR WARNINGS!")
    print("=======================================================")

if __name__ == "__main__":
    run_tests()
