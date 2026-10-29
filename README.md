<div align="center">

# ⚡ Grid Pulse: Cloud-Native Smart Grid Load Shedding Platform
### *End-to-End Big Data Engineering Pipeline on Microsoft Azure*

[![Big Data Track](https://img.shields.io/badge/Samsung%20Innovation%20Campus-SIC%208%20(2026)-blue?style=for-the-badge&logo=samsung)](https://samsung.com)
[![Azure Cloud](https://img.shields.io/badge/Cloud-Microsoft%20Azure-0089D6?style=for-the-badge&logo=microsoft-azure)](https://azure.microsoft.com)
[![Medallion Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%2FSilver%2FGold)-orange?style=for-the-badge)](https://www.databricks.com/glossary/medallion-architecture)

</div>

---

##  1. Business Context & Problem Statement
Power distribution management has emerged as a critical challenge for utility companies. Due to severe heatwaves and fuel shortages, national rolling blackouts (**"Load Shedding"**) are increasingly common. 

Conventional blind management leads to catastrophic consequences:
* **Risk to Critical Facilities:** Power is inadvertently cut off from hospitals, emergency clinics, and water treatment plants.
*  **Unequal Distribution:** Blackouts are distributed unevenly across neighborhoods.
*  **Hardware Damage:** Sudden voltage fluctuations (sags and spikes) when power is restored destroy expensive household appliances and sensitive medical equipment.

**The Solution:** *Grid Pulse* transitions utility providers from rigid, manual blackout schedules to a **Smart, Data-Driven Load Shedding Platform** that guarantees real-time grid visibility, protects critical infrastructure, and ensures equitable load distribution.

---

## 🏛️ 2. End-to-End Architecture & Data Flow
The platform is built entirely on **Microsoft Azure**, adopting the rigorous **Medallion Architecture**:

<img width="2752" height="1536" alt="final arc grid pulse_convert_png" src="https://github.com/user-attachments/assets/3c312693-55ba-42ed-87f9-cebf629e6e35" />


1. **Data Generation & Ingestion:** A multi-threaded Python script simulates 100,000+ smart meters and weather stations, streaming high-frequency telemetry into **Azure Event Hubs**.
2. **Stream Processing:** **Azure Stream Analytics** processes payloads on-the-fly, handling out-of-order events via *Watermarking* and outputting optimized **Parquet** files.
3. **Storage & Raw Zone:** Ingested raw telemetry lands immutably in **Azure Data Lake Storage Gen2 (ADLS Gen2)** under the `grid-raw-data` container.
4. **Transformation & Quality Gates:** **Azure Synapse Analytics (Serverless SQL Pools)** governs ELT transformations—applying `TRY_CAST` for fault tolerance and automated data quality assertions (e.g., isolating temperature anomalies into `NEEDS_TEMP_REVIEW`).
5. **Dimensional Modeling:** Curated data is structured into a conformed **Galaxy Schema (Fact Constellation)** to optimize analytical queries.
6. **Visualization & Decision Support:** **Power BI** connects via **DirectQuery** to deliver real-time operational KPIs to grid operators.

---

##  3. Key Data Engineering Highlights & Technical Excellence

* **Fault-Resilient Pipelines:** Replaced brittle casts with `TRY_CAST` across all Silver external tables to prevent batch failures during corrupt payloads.
* **Automated Data Quality Gates:** Embedded conditional business rules directly into transformation logic (e.g., detecting impossible thermal surges in weather streams).
* **Logical Lakehouse Separation:** Unified physical storage via ADLS Gen2 while maintaining strict logical directory segmentation (`lakehouse/silver/` and `lakehouse/gold/`).
* **Optimized Storage Formats:** Leveraged columnar **Parquet** storage for high-compression, partition pruning, and accelerated analytical read performance.
* **Orchestration Workflow:** Coordinated end-to-end task execution order (`run_silver_layer` ➔ `run_gold_dims` ➔ `run_gold_business`) via Azure Synapse dependency chaining.

---

## 📂 Repository Structure

```text
GridPulse-Azure-Data-Pipeline/
├── data_generator/         #  Python simulator script
├── scripts/                # Azure synapse transformations (Stream Analytics, Silver cleansing, Gold Galaxy Schema)
├── documentation/          # technical report & architectural blue-prints
├── presentation/           #  presentation  
└── demo/                   # Pipeline video 

```

---

## 4. Business Value & Dashboard Insights

The live Power BI operational command center empowers utility operators to:

* **Protect Critical Infrastructure:** Instantly isolate hospitals and emergency facilities from rolling blackouts using the *Customer Breakdown Matrix*.
* **Mitigate Grid Strain:** Track regional voltage sags (e.g., identifying stability drops in Giza) to deploy capacitor banks proactively.
* **Thermal Overload Watchlist:** Isolate assets flagged with `NEEDS_TEMP_REVIEW` to dispatch maintenance crews before insulation failure occurs.
* **Demand Forecasting:** Analyze daily cyclical surges via the *Peak Demand Timeline* to optimize peak-shaving reserves.
  
<img width="1121" height="628" alt="WhatsApp Image 2026-10-27 at 10 55 54 PM" src="https://github.com/user-attachments/assets/63668445-6753-4003-bb50-c79d1e7f6f51" />

---

*Developed with Shereen Mohamed as part of the Graduation project Big Data Track | Samsung Innovation Campus (2026)*
