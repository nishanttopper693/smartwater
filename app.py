"""
AQUAGUARD 360 | Smart Water Distribution & Leakage Detection System
Powered by Apache Kafka Streaming & Apache Spark Real-Time Analytics
Light & Simple UI Edition with Compact System Architecture & Live Event Telemetry
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time
import json
from datetime import datetime
from collections import deque

from pipeline_model import get_default_network_topology, NODE_COORDINATES, PipelineSegment
from kafka_engine import KafkaBrokerSimulator, generate_sensor_telemetry
from spark_detector import SparkStreamingDetector
from demonstration_engine import RuleBasedLeakageDetector, check_service_availability

# Page Configuration
st.set_page_config(
    page_title="AquaGuard 360 | Smart Water Distribution Dashboard",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Light and Simple UI Custom CSS
st.markdown("""
<style>
    :root {
        --bg-main: #F8FAFC;
        --bg-card: #FFFFFF;
        --border-color: #E2E8F0;
        --text-primary: #0F172A;
        --text-secondary: #475569;
        --text-muted: #64748B;
        --brand-blue: #0284C7;
        --brand-teal: #0D9488;
        --alert-amber: #D97706;
        --alert-red: #DC2626;
        --alert-green: #16A34A;
    }

    .main {
        background-color: #F8FAFC;
        color: #0F172A;
    }

    .stApp {
        background-color: #F8FAFC;
    }

    /* Top Header Bar */
    .header-bar {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 16px 24px;
        margin-bottom: 18px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 12px;
    }

    .header-title-box {
        display: flex;
        align-items: center;
        gap: 12px;
    }

    .header-icon {
        font-size: 28px;
    }

    .header-title {
        font-size: 21px;
        font-weight: 700;
        color: #0F172A;
        margin: 0;
        line-height: 1.2;
    }

    .header-subtitle {
        font-size: 12.5px;
        color: #64748B;
        margin-top: 2px;
    }

    /* KPI Cards */
    .kpi-card {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 16px 18px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
        transition: transform 0.15s ease, box-shadow 0.15s ease;
    }

    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 6px -1px rgba(0,0,0,0.08);
        border-color: #CBD5E1;
    }

    .kpi-label {
        font-size: 11.5px;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        color: #64748B;
        margin-bottom: 6px;
    }

    .kpi-value {
        font-size: 25px;
        font-weight: 800;
        color: #0284C7;
        line-height: 1.1;
    }

    .kpi-meta {
        font-size: 12px;
        color: #64748B;
        margin-top: 6px;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* Status Badges */
    .badge {
        display: inline-block;
        padding: 3px 9px;
        border-radius: 12px;
        font-size: 11px;
        font-weight: 600;
    }

    .badge-normal {
        background-color: #DCFCE7;
        color: #166534;
        border: 1px solid #BBF7D0;
    }

    .badge-warning {
        background-color: #FEF3C7;
        color: #92400E;
        border: 1px solid #FDE68A;
    }

    .badge-danger {
        background-color: #FEE2E2;
        color: #991B1B;
        border: 1px solid #FECACA;
    }

    .badge-simulated {
        background-color: #F1F5F9;
        color: #475569;
        border: 1px solid #CBD5E1;
        font-size: 11px;
        font-weight: 700;
    }

    /* Pipeline Architecture Component Box */
    .arch-container {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 16px 20px;
        margin-bottom: 20px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    }

    .pipeline-flow {
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 8px;
        flex-wrap: wrap;
        margin: 14px 0;
    }

    .pipeline-step {
        flex: 1;
        min-width: 170px;
        background-color: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 12px 14px;
        position: relative;
    }

    .pipeline-step-active {
        border-color: #0284C7;
        background-color: #F0F9FF;
    }

    .pipeline-arrow {
        color: #94A3B8;
        font-size: 18px;
        font-weight: bold;
    }

    /* Alert Banner */
    .alert-banner-candidate {
        background-color: #FFFBEB;
        border: 1px solid #FCD34D;
        border-left: 5px solid #D97706;
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 20px;
        color: #92400E;
    }

    .alert-banner-monitoring {
        background-color: #F0F9FF;
        border: 1px solid #BAE6FD;
        border-left: 5px solid #0284C7;
        border-radius: 8px;
        padding: 14px 18px;
        margin-bottom: 20px;
        color: #075985;
    }

    .alert-banner-normal {
        background-color: #F0FDF4;
        border: 1px solid #BBF7D0;
        border-left: 5px solid #16A34A;
        border-radius: 8px;
        padding: 12px 18px;
        margin-bottom: 20px;
        color: #166534;
        display: flex;
        align-items: center;
        gap: 10px;
    }

    .content-box {
        background-color: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 10px;
        padding: 18px 22px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.03);
        margin-bottom: 20px;
    }

    .content-box-title {
        font-size: 15px;
        font-weight: 700;
        color: #0F172A;
        margin-bottom: 12px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }

    .code-box {
        background-color: #0F172A;
        color: #E2E8F0;
        border-radius: 6px;
        padding: 12px 14px;
        font-family: 'Consolas', 'Courier New', monospace;
        font-size: 12px;
        overflow-x: auto;
        line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)

# Probe for genuine Kafka / Spark network services on localhost
is_kafka_live = check_service_availability("localhost", 9092)
is_spark_live = check_service_availability("localhost", 7077)

# Session State Initialization
if "rule_detector" not in st.session_state:
    st.session_state.rule_detector = RuleBasedLeakageDetector()

if "topology" not in st.session_state:
    st.session_state.topology = get_default_network_topology()

if "kafka_broker" not in st.session_state:
    st.session_state.kafka_broker = KafkaBrokerSimulator()

if "spark_engine" not in st.session_state:
    st.session_state.spark_engine = SparkStreamingDetector()

if "simulation_running" not in st.session_state:
    st.session_state.simulation_running = True

if "alert_history" not in st.session_state:
    st.session_state.alert_history = []

if "logged_incident_ids" not in st.session_state:
    st.session_state.logged_incident_ids = set()

if "total_readings_processed" not in st.session_state:
    st.session_state.total_readings_processed = 48  # Initial baseline readings seeded

# Helper Functions
def run_simulation_step():
    """Generates a new telemetry tick and processes through rule-based detector and Kafka/Spark."""
    readings, meta = st.session_state.rule_detector.generate_step_readings()
    candidate = meta.get("candidate_incident")

    st.session_state.total_readings_processed += len(readings)

    if candidate and candidate.incident_id not in st.session_state.logged_incident_ids:
        st.session_state.logged_incident_ids.add(candidate.incident_id)
        st.session_state.alert_history.insert(0, {
            "Timestamp": candidate.timestamp,
            "Incident ID": candidate.incident_id,
            "Location": candidate.location_name,
            "Flow Rate": f"{candidate.flow_reading:.1f} m³/h",
            "Pressure": f"{candidate.pressure_reading:.2f} bar",
            "Reason": candidate.reason_for_alert,
            "Status": candidate.incident_status
        })

    now = time.time()
    for seg in st.session_state.topology.values():
        telemetry = generate_sensor_telemetry(seg, now)
        st.session_state.kafka_broker.publish("water.telemetry.inlet", seg.inlet_sensor_flow_id, telemetry)
        st.session_state.kafka_broker.publish("water.telemetry.outlet", seg.outlet_sensor_flow_id, telemetry)

    return readings, meta

# Sidebar: Secondary Settings and Pipeline Info
with st.sidebar:
    st.markdown("### ⚙️ System Configuration")
    st.markdown("<small style='color:#64748B;'>AquaGuard 360 Local Prototype</small>", unsafe_allow_html=True)
    
    st.markdown("---")
    st.markdown("#### 📡 Service Probe Verification")
    st.markdown(f"""
    <div style="font-size:12px; color:#475569; line-height:1.7;">
        <b>Kafka Cluster (9092):</b> {'<span style="color:#16A34A;">ONLINE</span>' if is_kafka_live else '<span style="color:#D97706; font-weight:600;">SIMULATED LOCALLY</span>'}<br>
        <b>Spark Cluster (7077):</b> {'<span style="color:#16A34A;">ONLINE</span>' if is_spark_live else '<span style="color:#D97706; font-weight:600;">SIMULATED LOCALLY</span>'}<br>
        <span style="font-size:11px; color:#64748B;">No external JVM services required; standalone simulation engine handles producer-consumer queues and micro-batches.</span>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("#### 🚰 Isolation Valves")
    for seg_id, seg in st.session_state.topology.items():
        v1, v2 = st.columns([3, 2])
        with v1:
            st.markdown(f"**{seg.valve_id}**<br><small style='color:#64748B;'>{seg_id}</small>", unsafe_allow_html=True)
        with v2:
            if seg.valve_status == "OPEN":
                if st.button("Isolate", key=f"side_iso_{seg_id}", use_container_width=True):
                    seg.valve_status = "ISOLATED"
                    st.rerun()
            else:
                if st.button("Open", key=f"side_open_{seg_id}", use_container_width=True):
                    seg.valve_status = "OPEN"
                    st.rerun()

# Dashboard Auto-refresh fragment
@st.fragment(run_every="2s" if st.session_state.simulation_running else None)
def render_dashboard():
    if st.session_state.simulation_running:
        readings, meta = run_simulation_step()
    else:
        readings, meta = st.session_state.rule_detector.generate_step_readings()

    candidate = meta.get("candidate_incident")
    detector_state = meta.get("detector_state", "NORMAL")
    latest_evt = meta.get("latest_event", {})
    latest_det = meta.get("latest_detection", {})

    inlet_reading = readings["RES-INLET"]
    ds1_reading = readings["DS-LOC-01"]

    # Header Bar
    sim_status_badge = '<span class="badge badge-normal">● LIVE STREAMING</span>' if st.session_state.simulation_running else '<span class="badge badge-warning">⏸ PAUSED</span>'
    kafka_badge = '<span class="badge badge-normal">Kafka Service: Live</span>' if is_kafka_live else '<span class="badge badge-simulated">Kafka Role: Simulated Queue</span>'
    spark_badge = '<span class="badge badge-normal">Spark Service: Live</span>' if is_spark_live else '<span class="badge badge-simulated">Spark Role: Simulated Window</span>'

    st.markdown(f"""
    <div class="header-bar">
        <div class="header-title-box">
            <span class="header-icon">💧</span>
            <div>
                <div class="header-title">Smart Water Distribution & Leakage Detection</div>
                <div class="header-subtitle">Real-Time IoT Telemetry, Apache Kafka & Spark Architecture &nbsp;|&nbsp; <span class="badge badge-simulated">SIMULATED DATA</span></div>
            </div>
        </div>
        <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
            {sim_status_badge}
            {kafka_badge}
            {spark_badge}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Clean Controls Ribbon
    c_btn_leak, c_btn_reset, c_btn_pause, c_btn_step = st.columns([3, 3, 3, 2])
    
    with c_btn_leak:
        if st.button("🚨 Inject Simulated Leakage", use_container_width=True, type="primary" if meta["scenario"] != "SIMULATED_LEAKAGE" else "secondary"):
            st.session_state.rule_detector.set_scenario("SIMULATED_LEAKAGE")
            run_simulation_step()
            st.rerun()

    with c_btn_reset:
        if st.button("🔄 Reset to Normal Baseline", use_container_width=True, type="secondary" if meta["scenario"] == "SIMULATED_LEAKAGE" else "primary"):
            st.session_state.rule_detector.set_scenario("NORMAL")
            run_simulation_step()
            st.rerun()

    with c_btn_pause:
        if st.session_state.simulation_running:
            if st.button("⏸ Pause Simulation", use_container_width=True):
                st.session_state.simulation_running = False
                st.rerun()
        else:
            if st.button("▶ Resume Simulation", use_container_width=True, type="primary"):
                st.session_state.simulation_running = True
                st.rerun()

    with c_btn_step:
        if st.button("⚡ Step Once", use_container_width=True):
            run_simulation_step()
            st.rerun()

    st.markdown("<div style='height: 4px;'></div>", unsafe_allow_html=True)

    # 4 Main KPI Cards
    flow_val = inlet_reading.flow_m3h
    flow_color = "#DC2626" if flow_val >= 40.0 else "#0284C7"
    flow_delta_text = f"+{((flow_val-30.2)/30.2)*100:.1f}% surge" if flow_val > 35.0 else "Within nominal baseline"

    press_val = ds1_reading.pressure_bar
    press_color = "#DC2626" if press_val <= 3.55 else "#0D9488"
    press_delta_text = f"-{((4.18-press_val)/4.18)*100:.1f}% drop" if press_val <= 3.60 else "Optimal pressure"

    active_alerts_count = 1 if candidate else (1 if detector_state == "TRANSIENT_ANOMALY_MONITORING" else 0)
    alerts_color = "#D97706" if candidate else ("#0284C7" if active_alerts_count > 0 else "#16A34A")

    k1, k2, k3, k4 = st.columns(4)

    with k1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Current Inflow Rate [SIMULATED]</div>
            <div class="kpi-value" style="color: {flow_color};">{flow_val:.1f} <span style="font-size:14px; font-weight:500; color:#64748B;">m³/h</span></div>
            <div class="kpi-meta">
                <span>Baseline: 30.2 m³/h</span>
                &bull;
                <span style="color:{flow_color}; font-weight:600;">{flow_delta_text}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Downstream Pressure [SIMULATED]</div>
            <div class="kpi-value" style="color: {press_color};">{press_val:.2f} <span style="font-size:14px; font-weight:500; color:#64748B;">bar</span></div>
            <div class="kpi-meta">
                <span>Baseline: 4.18 bar</span>
                &bull;
                <span style="color:{press_color}; font-weight:600;">{press_delta_text}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with k3:
        status_word = "Candidate Alert Active" if candidate else ("Checking Persistence" if active_alerts_count > 0 else "All Normal")
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Active Alerts</div>
            <div class="kpi-value" style="color: {alerts_color};">{active_alerts_count} <span style="font-size:14px; font-weight:500; color:#64748B;">Incident</span></div>
            <div class="kpi-meta">
                <span style="color:{alerts_color}; font-weight:600;">{status_word}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with k4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Sensor Health</div>
            <div class="kpi-value" style="color: #16A34A;">100%</div>
            <div class="kpi-meta">
                <span>4/4 Telemetry Nodes Online</span>
                &bull;
                <span style="color:#16A34A; font-weight:600;">Telemetry Healthy</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 4px;'></div>", unsafe_allow_html=True)

    # Condition & Alert Banner
    if candidate:
        st.markdown(f"""
        <div class="alert-banner-candidate">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:10px;">
                <div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span class="badge badge-danger">⚠️ CANDIDATE LEAKAGE INCIDENT</span>
                        <span class="badge badge-simulated">UNCONFIRMED - REQUIRES FIELD VALIDATION</span>
                    </div>
                    <div style="font-size:15px; font-weight:700; color:#0F172A; margin-top:6px;">
                        Incident ID: {candidate.incident_id} &nbsp;|&nbsp; Detected Location: {candidate.location_name} ({candidate.location_id})
                    </div>
                    <div style="font-size:13px; color:#475569; margin-top:4px;">
                        <b>Reason for Alert:</b> {candidate.reason_for_alert}
                    </div>
                    <div style="font-size:12px; color:#92400E; margin-top:6px; font-weight:500;">
                        <i>Note: This is a <b>Candidate Alert</b>. By system protocol, it must NOT be presented as a confirmed leak until physical on-site or acoustic correlation validation has been completed by field dispatch crews.</i>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    elif detector_state == "TRANSIENT_ANOMALY_MONITORING":
        st.markdown(f"""
        <div class="alert-banner-monitoring">
            <div style="display:flex; align-items:center; gap:8px;">
                <span class="badge badge-warning">🔍 MONITORING PERSISTENCE (Sample 1 of 2)</span>
                <span class="badge badge-simulated">SIMULATED DATA</span>
            </div>
            <div style="font-size:14px; font-weight:700; color:#075985; margin-top:6px;">
                Flow increased to {inlet_reading.flow_m3h:.1f} m³/h and Downstream Pressure dropped to {ds1_reading.pressure_bar:.2f} bar.
            </div>
            <div style="font-size:12.5px; color:#475569; margin-top:4px;">
                <b>Persistence Check:</b> A candidate alert is suppressed on the first reading to prevent false alarms from transient water hammer. Monitoring consecutive sample...
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="alert-banner-normal">
            <span style="font-size:18px;">✓</span>
            <div style="font-size:13.5px; font-weight:600;">
                All pipeline locations operating within normal parameters &nbsp;|&nbsp; Baseline Flow: 30.2 m³/h &nbsp;|&nbsp; Baseline Pressure: 4.18 bar &nbsp;|&nbsp; No active candidate alerts.
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ----------------------------------------------------
    # COMPACT SYSTEM ARCHITECTURE SECTION (Requested)
    # ----------------------------------------------------
    st.markdown("""
    <div class="arch-container">
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
            <div style="font-size:15px; font-weight:700; color:#0F172A;">
                📐 System Architecture Pipeline (Project Report Specification)
            </div>
            <div style="font-size:11.5px; color:#64748B;">
                Telemetry Pipeline & State Flow
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Visual 5-Stage Pipeline Flow
    k_status_text = "Live Broker" if is_kafka_live else "Simulated In-Memory Queue"
    s_status_text = "Live Cluster" if is_spark_live else "Simulated Micro-Batch Window"

    st.markdown(f"""
    <div class="pipeline-flow">
        <div class="pipeline-step">
            <div style="font-size:11px; font-weight:700; color:#0284C7;">STAGE 1</div>
            <div style="font-size:13px; font-weight:700; color:#0F172A; margin:2px 0;">Simulated Water Sensors</div>
            <div style="font-size:11.5px; color:#64748B;">IoT Flow & Pressure Nodes</div>
            <div style="margin-top:6px;"><span class="badge badge-simulated">SIMULATED DATA</span></div>
        </div>
        <div class="pipeline-arrow">➔</div>
        <div class="pipeline-step">
            <div style="font-size:11px; font-weight:700; color:#0284C7;">STAGE 2</div>
            <div style="font-size:13px; font-weight:700; color:#0F172A; margin:2px 0;">Event Ingestion (Kafka role)</div>
            <div style="font-size:11.5px; color:#64748B;">Topic: <code>water.telemetry.inlet</code></div>
            <div style="margin-top:6px;"><span class="badge {'badge-normal' if is_kafka_live else 'badge-warning'}">{k_status_text}</span></div>
        </div>
        <div class="pipeline-arrow">➔</div>
        <div class="pipeline-step">
            <div style="font-size:11px; font-weight:700; color:#0284C7;">STAGE 3</div>
            <div style="font-size:13px; font-weight:700; color:#0F172A; margin:2px 0;">Stream Processing (Spark role)</div>
            <div style="font-size:11.5px; color:#64748B;">15s Sliding Window Batch</div>
            <div style="margin-top:6px;"><span class="badge {'badge-normal' if is_spark_live else 'badge-warning'}">{s_status_text}</span></div>
        </div>
        <div class="pipeline-arrow">➔</div>
        <div class="pipeline-step">
            <div style="font-size:11px; font-weight:700; color:#0284C7;">STAGE 4</div>
            <div style="font-size:13px; font-weight:700; color:#0F172A; margin:2px 0;">Leakage Detection</div>
            <div style="font-size:11.5px; color:#64748B;">Dual Check + ≥2 Samples</div>
            <div style="margin-top:6px;"><span class="badge badge-normal">Rule Engine Active</span></div>
        </div>
        <div class="pipeline-arrow">➔</div>
        <div class="pipeline-step">
            <div style="font-size:11px; font-weight:700; color:#0284C7;">STAGE 5</div>
            <div style="font-size:13px; font-weight:700; color:#0F172A; margin:2px 0;">Incident Dashboard</div>
            <div style="font-size:11.5px; color:#64748B;">Candidate Alert Protocol</div>
            <div style="margin-top:6px;"><span class="badge badge-normal">Streamlit UI</span></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Real vs Simulated Service Transparency Notice
    if not is_kafka_live and not is_spark_live:
        st.markdown("""
        <div style="background-color:#F8FAFC; border:1px solid #E2E8F0; border-radius:6px; padding:10px 14px; font-size:12px; color:#475569; margin-bottom:14px;">
            ℹ️ <b>Service Verification Note:</b> Genuine Apache Kafka (port 9092) and Apache Spark (port 7077) JVM services were probed and are <b>not running</b> on the local environment. A self-contained, working local simulation of the producer-consumer topic queues and stream-processing micro-batches is executing in their respective roles.
        </div>
        """, unsafe_allow_html=True)

    # Latest Processed Event & Detection Result
    col_evt, col_det = st.columns(2)
    with col_evt:
        st.markdown("**📨 Latest Processed Event (Kafka Ingestion Role - Simulated)**")
        st.markdown(f"""
        <div class="code-box">
{json.dumps(latest_evt, indent=2)}
        </div>
        """, unsafe_allow_html=True)

    with col_det:
        st.markdown("**⚡ Latest Detection Result (Spark Stream Processing Role - Simulated)**")
        st.markdown(f"""
        <div class="code-box">
{json.dumps(latest_det, indent=2)}
        </div>
        """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # Real-Time Updating Plotly Chart (Flow and Pressure)
    st.markdown("""
    <div class="content-box">
        <div class="content-box-title">
            <span>📈 Real-Time Pipeline Telemetry Chart (Flow & Downstream Pressure)</span>
            <span class="badge badge-simulated">SIMULATED TELEMETRY</span>
        </div>
    """, unsafe_allow_html=True)

    demo_hist = list(st.session_state.rule_detector.sensor_history)
    if demo_hist:
        df_chart = pd.DataFrame(demo_hist).tail(35)

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.12,
            subplot_titles=(
                "Reservoir Inlet Flow Rate (m³/h) — Baseline: 30.2 m³/h | Simulated Surge: 48.5 m³/h",
                "Downstream Location A Pressure (bar) — Baseline: 4.18 bar | Simulated Drop: 3.50 → 3.48 bar"
            )
        )

        fig.add_trace(
            go.Scatter(
                x=df_chart["time_str"],
                y=df_chart["inlet_flow"],
                name="Inlet Flow (m³/h)",
                line=dict(color="#0284C7", width=2.5),
                mode="lines+markers",
                marker=dict(size=6, color="#0284C7")
            ),
            row=1, col=1
        )
        fig.add_hline(
            y=30.2, line_dash="dash", line_color="#0D9488",
            annotation_text="Normal Baseline (30.2 m³/h)", annotation_position="top left",
            row=1, col=1
        )
        fig.add_hline(
            y=40.0, line_dash="dot", line_color="#D97706",
            annotation_text="High Flow Trigger Threshold (40.0 m³/h)", annotation_position="bottom right",
            row=1, col=1
        )

        fig.add_trace(
            go.Scatter(
                x=df_chart["time_str"],
                y=df_chart["ds_press_1"],
                name="Downstream Press A (bar)",
                line=dict(color="#DC2626", width=2.5),
                mode="lines+markers",
                marker=dict(size=6, color="#DC2626")
            ),
            row=2, col=1
        )
        fig.add_trace(
            go.Scatter(
                x=df_chart["time_str"],
                y=df_chart["inlet_pressure"],
                name="Inlet Pressure Ref (bar)",
                line=dict(color="#64748B", width=1.5, dash="dot"),
                mode="lines"
            ),
            row=2, col=1
        )
        fig.add_hline(
            y=4.18, line_dash="dash", line_color="#0D9488",
            annotation_text="Normal Baseline (4.18 bar)", annotation_position="top left",
            row=2, col=1
        )
        fig.add_hline(
            y=3.55, line_dash="dash", line_color="#D97706",
            annotation_text="Anomaly Persistence Limit (≤ 3.55 bar)", annotation_position="bottom right",
            row=2, col=1
        )

        fig.update_layout(
            paper_bgcolor="#FFFFFF",
            plot_bgcolor="#F8FAFC",
            font=dict(color="#334155", family="Segoe UI, -apple-system, sans-serif"),
            margin=dict(l=45, r=30, t=35, b=25),
            height=420,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        fig.update_yaxes(title_text="Flow (m³/h)", row=1, col=1, gridcolor="#E2E8F0", zerolinecolor="#E2E8F0")
        fig.update_yaxes(title_text="Pressure (bar)", row=2, col=1, gridcolor="#E2E8F0", zerolinecolor="#E2E8F0")
        fig.update_xaxes(gridcolor="#E2E8F0")

        st.plotly_chart(fig, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # 2 Column Section: Sensor Table (Left) & Alert History Table (Right)
    col_sensors, col_history = st.columns([5, 5])

    # SENSOR TABLE
    with col_sensors:
        st.markdown("""
        <div class="content-box">
            <div class="content-box-title">
                <span>📡 Sensor Telemetry by Location</span>
                <span class="badge badge-simulated">LIVE SAMPLING</span>
            </div>
        """, unsafe_allow_html=True)

        sensor_rows = []
        for loc_id, r in readings.items():
            if r.is_anomaly_sample:
                st_badge = "ANOMALY DROP" if r.pressure_bar <= 3.55 else "HIGH FLOW"
            else:
                st_badge = "NORMAL"

            sensor_rows.append({
                "Location": r.location_name,
                "Sensor ID": loc_id,
                "Flow (m³/h)": f"{r.flow_m3h:.2f}",
                "Pressure (bar)": f"{r.pressure_bar:.2f}",
                "Status": st_badge,
                "Health": "ONLINE (100%)",
                "Data": r.data_label
            })

        df_sensors = pd.DataFrame(sensor_rows)
        st.dataframe(
            df_sensors,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Status": st.column_config.TextColumn(
                    "Status",
                    help="Operating condition based on baseline evaluation"
                )
            }
        )
        st.markdown("</div>", unsafe_allow_html=True)

    # ALERT HISTORY TABLE
    with col_history:
        st.markdown("""
        <div class="content-box">
            <div class="content-box-title">
                <span>📋 Alert & Incident History</span>
                <span class="badge badge-simulated">TIMELOG</span>
            </div>
        """, unsafe_allow_html=True)

        if st.session_state.alert_history:
            df_hist_table = pd.DataFrame(st.session_state.alert_history)
            st.dataframe(
                df_hist_table[["Timestamp", "Incident ID", "Location", "Flow Rate", "Pressure", "Status"]],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.markdown("""
            <div style="background-color:#F8FAFC; border:1px dashed #CBD5E1; border-radius:6px; padding:24px; text-align:center; color:#64748B; font-size:13px;">
                No candidate alerts logged in the current session.<br>
                Click <b>'Inject Simulated Leakage'</b> above to simulate a rupture and observe the detector evaluate persistence.
            </div>
            """, unsafe_allow_html=True)

        st.markdown("</div>", unsafe_allow_html=True)

    # ----------------------------------------------------
    # DEMO RESULTS SECTION (Actual Outputs from Simulation)
    # ----------------------------------------------------
    st.markdown("""
    <div class="content-box">
        <div class="content-box-title">
            <span>📊 Simulation Demo Results (Observed Local Test Outputs)</span>
            <span class="badge badge-simulated">ACTUAL SESSION DATA</span>
        </div>
        <div style="font-size:12.5px; color:#64748B; margin-bottom:14px;">
            The metrics below reflect genuine outputs gathered directly from the current active simulation run.
        </div>
    """, unsafe_allow_html=True)

    # Key Demo Outputs Grid
    res_col1, res_col2, res_col3, res_col4 = st.columns(4)

    with res_col1:
        st.markdown(f"""
        <div style="background-color:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:12px 14px;">
            <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase;">Readings Processed</div>
            <div style="font-size:22px; font-weight:800; color:#0284C7; margin:4px 0;">{st.session_state.total_readings_processed:,}</div>
            <div style="font-size:11.5px; color:#475569;">Total sensor telemetry samples</div>
        </div>
        """, unsafe_allow_html=True)

    with res_col2:
        cand_count = len(st.session_state.alert_history)
        st.markdown(f"""
        <div style="background-color:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:12px 14px;">
            <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase;">Candidate Alerts</div>
            <div style="font-size:22px; font-weight:800; color:{'#D97706' if cand_count > 0 else '#16A34A'}; margin:4px 0;">{cand_count}</div>
            <div style="font-size:11.5px; color:#475569;">Verified persistent incidents</div>
        </div>
        """, unsafe_allow_html=True)

    with res_col3:
        st.markdown("""
        <div style="background-color:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:12px 14px;">
            <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase;">Normal Baseline</div>
            <div style="font-size:15px; font-weight:700; color:#0D9488; margin:6px 0;">30.2 m³/h &nbsp;|&nbsp; 4.18 bar</div>
            <div style="font-size:11.5px; color:#475569;">Target nominal operating envelope</div>
        </div>
        """, unsafe_allow_html=True)

    with res_col4:
        st.markdown("""
        <div style="background-color:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:12px 14px;">
            <div style="font-size:11px; font-weight:700; color:#64748B; text-transform:uppercase;">Simulated Abnormal</div>
            <div style="font-size:15px; font-weight:700; color:#DC2626; margin:6px 0;">48.5 m³/h &nbsp;|&nbsp; 3.50 → 3.48 bar</div>
            <div style="font-size:11.5px; color:#475569;">Observed surge & consecutive drop</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<div style='height: 12px;'></div>", unsafe_allow_html=True)

    # Detailed Alert Generation Timing and Reason
    if st.session_state.alert_history:
        latest_logged = st.session_state.alert_history[0]
        st.markdown(f"""
        <div style="background-color:#FFFBEB; border:1px solid #FCD34D; border-left:4px solid #D97706; border-radius:6px; padding:12px 16px; margin-bottom:14px;">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span style="font-size:13px; font-weight:700; color:#92400E;">
                    ⚡ Most Recent Candidate Alert Generated: {latest_logged['Incident ID']}
                </span>
                <span style="font-size:12px; color:#78350F; font-weight:600;">Time Generated: {latest_logged['Timestamp']}</span>
            </div>
            <div style="font-size:12.5px; color:#451A03; margin-top:4px;">
                <b>Affected Location:</b> {latest_logged['Location']} &nbsp;|&nbsp; 
                <b>Observed Readings:</b> Flow {latest_logged['Flow Rate']}, Pressure {latest_logged['Pressure']}
            </div>
            <div style="font-size:12px; color:#78350F; background:rgba(255,255,255,0.7); border-radius:4px; padding:6px 10px; margin-top:6px;">
                <b>Detection Reason:</b> {latest_logged['Reason']}
            </div>
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background-color:#F8FAFC; border:1px dashed #CBD5E1; border-radius:6px; padding:12px 16px; margin-bottom:14px; font-size:12.5px; color:#64748B;">
            ℹ️ <b>Alert Generation Status:</b> No candidate alerts generated yet in this session. Click <b>'Inject Simulated Leakage'</b> above to trigger the simulated flow surge and consecutive pressure drop.
        </div>
        """, unsafe_allow_html=True)

    # Comparison Chart: Normal Baseline vs Simulated Abnormal Condition
    st.markdown("##### 📊 Comparison: Normal vs. Simulated Abnormal Flow and Pressure")

    fig_comp = go.Figure()

    # Flow Bars
    fig_comp.add_trace(go.Bar(
        name="Flow Rate (m³/h)",
        x=["Normal Baseline", "Simulated Abnormal (Sample 1)", "Simulated Abnormal (Sample 2)"],
        y=[30.2, 48.5, 48.5],
        text=["30.2 m³/h", "48.5 m³/h (+60.6%)", "48.5 m³/h (+60.6%)"],
        textposition="auto",
        marker_color=["#0284C7", "#DC2626", "#DC2626"],
        yaxis="y1"
    ))

    # Pressure Bars
    fig_comp.add_trace(go.Bar(
        name="Downstream Pressure (bar)",
        x=["Normal Baseline", "Simulated Abnormal (Sample 1)", "Simulated Abnormal (Sample 2)"],
        y=[4.18, 3.50, 3.48],
        text=["4.18 bar", "3.50 bar (-16.3%)", "3.48 bar (-16.7%)"],
        textposition="auto",
        marker_color=["#0D9488", "#F59E0B", "#D97706"],
        yaxis="y2"
    ))

    fig_comp.update_layout(
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#F8FAFC",
        font=dict(color="#334155", family="Segoe UI, -apple-system, sans-serif"),
        barmode="group",
        height=320,
        margin=dict(l=40, r=40, t=30, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis=dict(
            title=dict(text="Flow Rate (m³/h)", font=dict(color="#0284C7")),
            tickfont=dict(color="#0284C7"),
            gridcolor="#E2E8F0"
        ),
        yaxis2=dict(
            title=dict(text="Downstream Pressure (bar)", font=dict(color="#0D9488")),
            tickfont=dict(color="#0D9488"),
            overlaying="y",
            side="right",
            gridcolor="#F1F5F9"
        ),
        xaxis=dict(gridcolor="#E2E8F0")
    )

    st.plotly_chart(fig_comp, use_container_width=True)

    # ----------------------------------------------------
    # SEPARATE REAL-WORLD CONTEXT (PURI REPORT RESULT)
    # Strictly segregated from local simulated test outputs
    # ----------------------------------------------------
    st.markdown("""
    <div style="background-color:#F1F5F9; border:1px solid #CBD5E1; border-left:4px solid #475569; border-radius:6px; padding:12px 16px; margin-top:14px;">
        <div style="font-size:12px; font-weight:700; color:#1E293B; text-transform:uppercase; letter-spacing:0.5px;">
            🌍 External Case Study Context: Puri Municipality (Report Reference)
        </div>
        <div style="font-size:12.5px; color:#334155; margin-top:4px; line-height:1.6;">
            In published municipal water management studies (Puri 24x7 Drink-From-Tap initiative), non-revenue water (NRW) was reportedly reduced from <b>47% to 15%</b> through physical metering, district metered areas (DMA) zoning, and infrastructure renewal.
        </div>
        <div style="font-size:11.5px; color:#64748B; margin-top:6px; font-style:italic;">
            ⚠️ <b>Clarification Note:</b> This historical municipal outcome was achieved through physical infrastructure rehabilitation and standard district metering; it was <b>not established as a result of Apache Kafka or Apache Spark</b>. Local simulation results above are kept strictly independent from published municipal field outcomes.
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("</div>", unsafe_allow_html=True)

    # Collapsible Advanced Network Schematic
    with st.expander("🗺️ Municipal Pipeline Grid Schematic (5-Zone Topology)", expanded=False):

        fig_net = go.Figure()

        for seg_id, seg in st.session_state.topology.items():
            p1 = NODE_COORDINATES[seg.source_node]
            p2 = NODE_COORDINATES[seg.target_node]

            color = "#DC2626" if seg.active_leak_severity == "BURST" else ("#D97706" if seg.active_leak_severity != "NONE" else "#0D9488")
            fig_net.add_trace(go.Scatter(
                x=[p1["x"], p2["x"]],
                y=[p1["y"], p2["y"]],
                mode="lines+text",
                line=dict(color=color, width=4),
                text=["", seg_id],
                textposition="top center",
                hovertext=f"<b>{seg.name}</b><br>Nominal Flow: {seg.nominal_flow_lpm} L/min",
                hoverinfo="text",
                showlegend=False
            ))

        node_x = [info["x"] for info in NODE_COORDINATES.values()]
        node_y = [info["y"] for info in NODE_COORDINATES.values()]
        node_names = list(NODE_COORDINATES.keys())

        fig_net.add_trace(go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            marker=dict(size=18, color="#0284C7", line=dict(width=2, color="#FFFFFF")),
            text=node_names,
            textposition="bottom center",
            textfont=dict(color="#0F172A", size=10),
            showlegend=False
        ))

        fig_net.update_layout(
            paper_bgcolor="#FFFFFF",
            plot_bgcolor="#F8FAFC",
            margin=dict(l=10, r=10, t=10, b=20),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[0, 11]),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False, range=[1.5, 8.5]),
            height=280
        )
        st.plotly_chart(fig_net, use_container_width=True)

# Render Dashboard
render_dashboard()
