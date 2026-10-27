"""
Apache Kafka Streaming Ingestion Engine
Simulates high-throughput distributed message streaming for IoT flow & pressure sensors.
Follows Apache Kafka topic architecture, message schema, partition distribution, and consumer groups.
"""

import time
import random
import json
from collections import deque
from typing import Dict, List, Any, Optional

class KafkaMessage:
    def __init__(self, topic: str, key: str, value: dict, partition: int = 0):
        self.topic = topic
        self.key = key
        self.value = value
        self.partition = partition
        self.timestamp = time.time()
        self.offset = 0

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "key": self.key,
            "timestamp": self.timestamp,
            "payload": self.value
        }

class KafkaBrokerSimulator:
    """
    Lightweight, high-speed Apache Kafka Broker simulation.
    Manages topics, partitioned message buffers, consumer offsets, and streaming throughput.
    """
    def __init__(self, max_buffer_size: int = 500):
        self.max_buffer_size = max_buffer_size
        self.topics = {
            "water.telemetry.inlet": deque(maxlen=max_buffer_size),
            "water.telemetry.outlet": deque(maxlen=max_buffer_size),
            "water.alerts.leakage": deque(maxlen=max_buffer_size),
            "water.control.valves": deque(maxlen=max_buffer_size),
            "water.quarantine": deque(maxlen=max_buffer_size)
        }
        self.offsets: Dict[str, int] = {t: 0 for t in self.topics}
        self.total_messages_published = 0
        self.broker_id = "kafka-broker-node-01.smartwater.net:9092"
        self.cluster_id = "KAFKA-SW-DISTRIB-PROD-01"
        self.start_time = time.time()

    def publish(self, topic: str, key: str, value: dict, partition: int = 0) -> KafkaMessage:
        if topic not in self.topics:
            self.topics[topic] = deque(maxlen=self.max_buffer_size)
            self.offsets[topic] = 0

        msg = KafkaMessage(topic, key, value, partition)
        msg.offset = self.offsets[topic]
        self.offsets[topic] += 1
        self.topics[topic].append(msg)
        self.total_messages_published += 1
        return msg

    def consume_latest(self, topic: str, count: int = 10) -> List[KafkaMessage]:
        if topic not in self.topics:
            return []
        queue = list(self.topics[topic])
        return queue[-count:] if len(queue) >= count else queue

    def get_topic_stats(self) -> Dict[str, Any]:
        uptime_sec = max(1.0, time.time() - self.start_time)
        return {
            "broker": self.broker_id,
            "cluster_id": self.cluster_id,
            "total_messages": self.total_messages_published,
            "throughput_msg_per_sec": round(self.total_messages_published / uptime_sec, 1),
            "topics": {
                name: {
                    "current_offset": self.offsets[name],
                    "buffer_count": len(self.topics[name]),
                    "partitions": 3
                }
                for name in self.topics
            }
        }

def generate_sensor_telemetry(segment, timestamp: float, noise_scale: float = 0.015) -> Dict[str, Any]:
    """
    Simulates real-world physical hydraulics:
    - Normal pipe: Q_in ≈ Q_out, slight frictional pressure drop.
    - Valve throttled or isolated: Q falls to 0.
    - Leak: Q_in increases slightly due to pressure differential, Q_out drops significantly,
      pressure drops abruptly at both ends with higher gradient.
    """
    # Diurnal flow wave (simulating human water usage peaks at morning 7-9am and evening 6-9pm)
    hour = time.localtime(timestamp).tm_hour + time.localtime(timestamp).tm_min / 60.0
    diurnal_factor = 1.0 + 0.18 * random.choice([-1, 1]) * (0.5 * (1 + random.uniform(-0.1, 0.1)))

    nominal_flow = segment.nominal_flow_lpm * diurnal_factor
    nominal_press = segment.nominal_pressure_bar

    # If valve is closed
    if segment.valve_status == "ISOLATED":
        return {
            "inlet_flow": 0.0,
            "outlet_flow": 0.0,
            "delta_flow": 0.0,
            "inlet_pressure": round(nominal_press * 0.2 + random.uniform(-0.05, 0.05), 2),
            "outlet_pressure": 0.0,
            "leak_rate": 0.0,
            "status": "ISOLATED"
        }

    # Normal noise
    q_noise_in = random.gauss(0, nominal_flow * noise_scale)
    q_noise_out = random.gauss(0, nominal_flow * noise_scale)
    p_noise_in = random.gauss(0, 0.04)
    p_noise_out = random.gauss(0, 0.04)

    # Base physics
    # Friction head loss across length: ~0.15 bar per km for high flow
    friction_drop = 0.12 * segment.length_km

    inlet_flow = nominal_flow + q_noise_in
    inlet_pressure = nominal_press + p_noise_in

    # Calculate leak impact if any
    leak_rate = 0.0
    if segment.active_leak_severity == "SEEPAGE":
        leak_rate = random.uniform(15.0, 30.0) # LPM
    elif segment.active_leak_severity == "MODERATE":
        leak_rate = random.uniform(45.0, 95.0) # LPM
    elif segment.active_leak_severity == "BURST":
        leak_rate = random.uniform(180.0, 340.0) # LPM

    # Outlet flow equals inlet minus leak
    outlet_flow = max(0.0, inlet_flow - leak_rate + q_noise_out)

    # Pressure drops sharply during leak due to fluid escape velocity
    pressure_leak_drop = (leak_rate / max(1.0, nominal_flow)) * 1.8
    outlet_pressure = max(0.5, inlet_pressure - friction_drop - pressure_leak_drop + p_noise_out)
    if leak_rate > 0:
        inlet_pressure = max(1.0, inlet_pressure - (pressure_leak_drop * 0.45))

    delta_flow = max(0.0, inlet_flow - outlet_flow)

    status = "NORMAL"
    if segment.active_leak_severity != "NONE":
        status = "LEAK_DETECTED"
    elif segment.valve_status == "THROTTLED":
        status = "THROTTLED"

    return {
        "inlet_flow": round(inlet_flow, 2),
        "outlet_flow": round(outlet_flow, 2),
        "delta_flow": round(delta_flow, 2),
        "inlet_pressure": round(inlet_pressure, 2),
        "outlet_pressure": round(outlet_pressure, 2),
        "leak_rate": round(leak_rate, 2),
        "status": status
    }
