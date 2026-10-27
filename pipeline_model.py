"""
Smart Water Distribution Network Model
Defines the topology, pipeline segments, sensor nodes, and hydraulic characteristics
for the smart water distribution grid.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import time

@dataclass
class Sensor:
    sensor_id: str
    sensor_type: str  # 'FLOW' or 'PRESSURE'
    location: str
    unit: str
    nominal_value: float
    min_threshold: float
    max_threshold: float
    battery_level: float = 98.5
    status: str = "ONLINE"  # ONLINE, DEGRADED, OFFLINE

@dataclass
class PipelineSegment:
    segment_id: str
    name: str
    source_node: str
    target_node: str
    diameter_mm: int
    length_km: float
    nominal_flow_lpm: float       # Liters per minute
    nominal_pressure_bar: float    # Bar
    inlet_sensor_flow_id: str
    outlet_sensor_flow_id: str
    inlet_sensor_press_id: str
    outlet_sensor_press_id: str
    valve_id: str
    valve_status: str = "OPEN"     # OPEN, THROTTLED, ISOLATED
    active_leak_severity: str = "NONE" # NONE, SEEPAGE, MODERATE, BURST
    leak_rate_lpm: float = 0.0
    leak_distance_km: float = 0.0
    accumulated_loss_liters: float = 0.0

def get_default_network_topology() -> Dict[str, PipelineSegment]:
    """Returns the municipal water pipeline distribution network topology."""
    segments = {
        "SEC-01": PipelineSegment(
            segment_id="SEC-01",
            name="Main Transmission Trunk",
            source_node="Reservoir Alpha",
            target_node="Central Pumping Hub",
            diameter_mm=600,
            length_km=4.8,
            nominal_flow_lpm=1250.0,
            nominal_pressure_bar=6.2,
            inlet_sensor_flow_id="FLW-01-IN",
            outlet_sensor_flow_id="FLW-01-OUT",
            inlet_sensor_press_id="PRS-01-IN",
            outlet_sensor_press_id="PRS-01-OUT",
            valve_id="VLV-01-MAIN"
        ),
        "SEC-02": PipelineSegment(
            segment_id="SEC-02",
            name="Commercial District Ring",
            source_node="Central Pumping Hub",
            target_node="Commercial Plaza & Downtown",
            diameter_mm=400,
            length_km=3.2,
            nominal_flow_lpm=520.0,
            nominal_pressure_bar=4.8,
            inlet_sensor_flow_id="FLW-02-IN",
            outlet_sensor_flow_id="FLW-02-OUT",
            inlet_sensor_press_id="PRS-02-IN",
            outlet_sensor_press_id="PRS-02-OUT",
            valve_id="VLV-02-COMM"
        ),
        "SEC-03": PipelineSegment(
            segment_id="SEC-03",
            name="Highland Residential Sub-grid",
            source_node="Central Pumping Hub",
            target_node="Highland Residential District",
            diameter_mm=350,
            length_km=2.7,
            nominal_flow_lpm=410.0,
            nominal_pressure_bar=5.4,
            inlet_sensor_flow_id="FLW-03-IN",
            outlet_sensor_flow_id="FLW-03-OUT",
            inlet_sensor_press_id="PRS-03-IN",
            outlet_sensor_press_id="PRS-03-OUT",
            valve_id="VLV-03-RES"
        ),
        "SEC-04": PipelineSegment(
            segment_id="SEC-04",
            name="Tech & Industrial Corridor",
            source_node="Central Pumping Hub",
            target_node="Industrial Tech Park",
            diameter_mm=450,
            length_km=5.1,
            nominal_flow_lpm=680.0,
            nominal_pressure_bar=5.0,
            inlet_sensor_flow_id="FLW-04-IN",
            outlet_sensor_flow_id="FLW-04-OUT",
            inlet_sensor_press_id="PRS-04-IN",
            outlet_sensor_press_id="PRS-04-OUT",
            valve_id="VLV-04-IND"
        ),
        "SEC-05": PipelineSegment(
            segment_id="SEC-05",
            name="South Municipal Loop",
            source_node="Commercial Plaza & Downtown",
            target_node="South Suburb Expansion",
            diameter_mm=300,
            length_km=2.1,
            nominal_flow_lpm=290.0,
            nominal_pressure_bar=3.9,
            inlet_sensor_flow_id="FLW-05-IN",
            outlet_sensor_flow_id="FLW-05-OUT",
            inlet_sensor_press_id="PRS-05-IN",
            outlet_sensor_press_id="PRS-05-OUT",
            valve_id="VLV-05-SOUTH"
        )
    }
    return segments

# Coordinates for 2D Schematic Visualization
NODE_COORDINATES = {
    "Reservoir Alpha": {"x": 1.0, "y": 5.0, "type": "RESERVOIR"},
    "Central Pumping Hub": {"x": 4.0, "y": 5.0, "type": "PUMP_STATION"},
    "Commercial Plaza & Downtown": {"x": 7.0, "y": 7.0, "type": "DISTRICT"},
    "Highland Residential District": {"x": 7.0, "y": 5.0, "type": "DISTRICT"},
    "Industrial Tech Park": {"x": 7.0, "y": 3.0, "type": "INDUSTRIAL"},
    "South Suburb Expansion": {"x": 9.5, "y": 7.0, "type": "DISTRICT"},
}
