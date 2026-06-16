# High-Throughput Real-Time Streaming Pipeline (Weverse-Scale Simulation)
## 📖 The Origin Story: From a Crashed BTS Live to System Design

This project didn't start in a text editor; it started out of personal frustration. 

I was attempting to tune into a live BTS broadcast on Weverse when the application suddenly choked, threw errors, and completely kicked me out of the stream. When I finally managed to re-authenticate and log back in, I looked at the interface: millions of global fans were joining, leaving, sending messages, and spamming heart interactions simultaneously. The pure volume of concurrent telemetry data was staggering. 

As an engineer, my curiosity sparked. I didn't just want the stream to work, I wanted to understand exactly *why* it broke and *how* a system could be engineered to handle that much massive, unpredictable scale. 

###  💡 The Core Philosophy: Code is the Base, Cloud is the Tool
When breaking down this problem, I leaned heavily on a fundamental engineering truth which I learnt by engineers who do this successfully and brilliantly: **Clean, optimized algorithmic code is the foundation of everything; the cloud is merely a tool to scale that foundation.** If your core code structure is poorly designed, throwing expensive cloud resources at it won't fix the problem—it will just make your cloud bill incredibly expensive. Therefore, before spending a single dollar on cloud infrastructure, I challenged myself to build a local, cloud-native prototype that solves the scale problem fundamentally through software architecture. If this code can efficiently compress thousands of concurrent operations on a standard local machine, it will scale infinitely and cost-effectively when deployed onto enterprise cloud infrastructure.

## ☁️ System Architecture Overview
This repository contains a production-grade, horizontally scalable real-time streaming data pipeline designed to ingest, buffer, aggregate, and persist high-concurrency client telemetry data under intense traffic spikes (simulating global live-stream viewer events on a platform like Weverse).

The core engineering focus of this project is **database preservation via stateful in-memory window aggregation**. By decoupling the ingestion tier from the storage tier and micro-batching records in memory, the system mitigates database write-storms, connection pool starvation, and disk I/O bottlenecks.

### 🛠️ Local to GCP Production Mapping
To demonstrate cloud-native enterprise readiness, every component of this local decoupled layout maps directly to an industry-standard Google Cloud Platform service:

* **Load Simulation:** Locust Swarm Distributed Engine ➔ **GCP Cloud Tasks / Client App Traffic**
* **Ingestion Layer:** Asynchronous FastAPI Gateway ➔ **Google Cloud Run (Serverless GKE pods)**
* **Buffer / Shock Absorber:** Thread-Safe Volatile TCP Socket ➔ **Google Cloud Pub/Sub Topic**
* **Stream Processing Core:** Stateful In-Memory Aggregator Worker ➔ **Google Cloud Dataflow (Apache Beam)**
* **Storage Layer:** Relational PostgreSQL Cluster ➔ **Google Cloud SQL (PostgreSQL Instance)**
* **Operations Suite UI:** Real-Time Streamlit Live Dashboard ➔ **Google Looker Studio / Cloud Monitoring**

---

## ⚡ The Engineering Problem: Database Write-Storms
Under high-concurrency scenarios (e.g., millions of fans simultaneously joining a live broadcast), streaming raw network payloads directly into a relational database creates a massive performance bottleneck:
1.  **Connection Pool Saturation:** Each individual request attempts to hold a database worker thread, exhausting the maximum allowable connections instantly.
2.  **Lock Contention:** Thousands of concurrent `INSERT` statements executing against the same table cause index block locks, driving API response latency through the roof.
3.  **Silent Packet Drops:** Backpressure from the storage layer forces the ingestion API to time out, causing critical business telemetry data to be permanently lost.

### 💡 The Solution: Stateful 10s Tumbling Windows
Rather than executing an I/O disk operation for every network packet, this architecture introduces a **Stateful Compute Worker**. 

Incoming events are captured rapidly by a non-blocking background network socket queue. The worker consumes these items and buffers them inside an optimized volatile memory dictionary, grouping counts by `stream_id` and rounding them into flat **10-second fixed tumbling windows**. 

Every 10 seconds, the worker flushes the compiled state to PostgreSQL via a single, highly optimized batch `UPSERT` transaction utilizing an idempotent `ON CONFLICT DO UPDATE` constraint. **This compresses over 6,000+ distinct database operations into 1 single database transaction per window.**

---

## Tech Stack & Subsystems
* **Language:** Python 3.12 (Strongly Typed Paradigms)
* **Ingestion Gateway:** FastAPI (Asynchronous Event Loop)
* **Data Pipeline Infrastructure:** Threading, Queue Data Buffers, TCP Sockets
* **Database Target:** PostgreSQL (Psycopg2 Advanced Driver Client)
* **Performance Testing Engine:** Locust Load Testing Framework
* **Business Intelligence / Observability:** Streamlit Core UI Data Visualization

---

## Step-by-Step Deployment Guide

### 1. Database Initialization
Log into your local PostgreSQL instance and set up the target analytical table structure:
```sql
CREATE DATABASE weverse_analytics;
\c weverse_analytics;

CREATE TABLE live_stream_metrics (
    id SERIAL PRIMARY KEY,
    stream_id VARCHAR(255) NOT NULL,
    unique_fans INTEGER NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    CONSTRAINT unique_stream_window UNIQUE (stream_id, calculated_at)
);
```

### 2. Environment Configuration
Clone the repository and install the production package dependencies inside an isolated virtual environment:

```bash
git clone [https://github.com/Divya-A10/weverse-scale-streaming-pipeline.git](https://github.com/Divya-A10/weverse-scale-streaming-pipeline.git)
cd weverse-scale-streaming-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Pipeline Ingestion & Compute Core Execution
Spin up the background TCP network socket listener and the stateful in-memory batch processing aggregator:

```bash
python stream_processor.py
```

### 4. Operations Suite Monitoring Interface
Launch the real-time Google Cloud Platform styled monitoring console to watch live execution graphs:

```bash
streamlit run dashboard.py
```

### 5. High-Load Stress Testing
Execute your Locust file to orchestrate a high-concurrency distributed swarm against the pipeline:

```bash
locust -f locustfile.py
```
Open http://localhost:8089, set the target load to 1,000+ users, and observe the live traffic spikes collapse cleanly into optimized, perfectly rounded 10-second transactional blocks on your dashboard!

