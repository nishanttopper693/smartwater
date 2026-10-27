# Smart Water Distribution & Leakage Detection System
### Real-Time Monitoring & Anomaly Detection Using Apache Kafka & Apache Spark Roles

A high-performance working prototype for municipal water pipeline monitoring, mass-balance conservation tracking, explainable rule-based anomaly detection, and Candidate Incident lifecycle management.

---

## 🌟 Key Sections in Dashboard

### 1. Demo Results (Actual Simulation Outputs)
- **Observed Outputs from Current Run:**
  - **Readings Processed:** Live count of generated telemetry samples evaluated by the pipeline.
  - **Candidate Alerts Detected:** Count of verified persistent anomalies ($\ge 2$ consecutive readings).
  - **Normal Baseline vs. Simulated Abnormal:**
    - Normal Baseline: Flow `30.2 m³/h`, Pressure `4.18 bar`.
    - Simulated Abnormal Condition: Inflow surges to `48.5 m³/h` (+60.6%), Downstream Pressure drops to `3.50 bar` (Reading 1) and persists at `3.48 bar` (Reading 2, -16.7%).
  - **Latest Alert Details:** Shows the exact time the candidate incident was logged, the affected location, and the rule-based detection reason.
  - **Comparison Chart:** Grouped bar chart comparing normal vs. abnormal flow rates and pressures with direct data labels.
- **External Real-World Context (Puri Case Study):**
  - Includes historical context from the Puri 24x7 Drink-From-Tap initiative (non-revenue water reportedly reduced from **47% to 15%**).
  - Explicitly states that this historical result was achieved via municipal infrastructure renewal and DMA zoning, and **not established as a result of Apache Kafka or Apache Spark**.
  - Local simulated test results are kept strictly separate from published municipal outcomes, with zero invented benchmarks.

### 2. Compact System Architecture Pipeline
- **Visual 5-Stage Dataflow:**
  $$\text{Simulated Water Sensors} \longrightarrow \text{Event Ingestion (Kafka role)} \longrightarrow \text{Stream Processing (Spark role)} \longrightarrow \text{Leakage Detection} \longrightarrow \text{Incident Dashboard}$$
- **Service Transparency:**
  - Probes ports 9092 (Kafka) and 7077 (Spark).
  - Clearly labels components as **Simulated In-Memory Queues** and **Micro-Batch Engines** when external JVM services are not detected.
- **Live Event Inspection:** Exposes the raw JSON payload of the latest processed event and the latest detection result.

### 3. Light & Simple Dashboard UI
- **KPI Cards:** Current Inflow Rate (`m³/h`), Downstream Pressure (`bar`), Active Alerts, and Sensor Health (`100%`).
- **Real-Time Plotly Chart:** Dual synchronized subplots with baseline guides and threshold limit lines.
- **Sensor Table:** Location, Sensor ID, Flow, Pressure, Status, Health, and Data Label.
- **Alert History Table:** Logs timestamps, Incident IDs, locations, and explainable reasons for every alert.
- **Action Buttons:** Immediate triggers to inject simulated leakage, reset to normal baseline, pause/resume simulation, and advance single steps.

---

## 🚀 How to Run Locally

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Launch the Application
```bash
streamlit run app.py
```

The application is live locally at `http://localhost:8501`.

---

## 📁 Project Structure

- `app.py`: Streamlit dashboard with light UI, compact System Architecture section, Demo Results section, live event inspector, KPI cards, real-time Plotly charts, and sensor table.
- `demonstration_engine.py`: Explainable rule-based leakage detector with baseline (30.2 m³/h, 4.18 bar), abnormal condition (48.5 m³/h, 3.50 → 3.48 bar), persistence verification, service probe, and event serialization.
- `pipeline_model.py`: Water grid topology definition, pipeline characteristics, and sensor nodes.
- `kafka_engine.py`: Distributed messaging broker simulation (topics, partitions, offsets).
- `spark_detector.py`: Sliding-window mass balance and hydraulic anomaly detection engine.
- `requirements.txt`: Python dependencies (`streamlit`, `pandas`, `plotly`, `numpy`).
