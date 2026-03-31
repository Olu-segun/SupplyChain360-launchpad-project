# SupplyChain360 Unified Data Platform
---
## 📌 Project Overview
SupplyChain360 is a fast‑growing retail distribution company in the United States. The business faces operational inefficiencies such as frequent stockouts, overstocked warehouses, and delivery delays. Data fragmentation across multiple systems (store location, warehouses, suppliers, shipments, sales, warehouse and product) makes it difficult for leadership to gain timely insights.

This project builds a Unified Supply Chain Data Platform to centralize operational data, improve analytics, and enable better decision‑making in inventory planning, supplier performance monitoring, shipment tracking, and demand forecasting.
### The platform enables:
- 📦 **Inventory optimization**
- 🚚 **Shipment tracking**
- 🏭 **Warehouse efficiency analysis**
- 🤝 **Supplier performance monitoring**
---
## 🧠 Business Problem
SupplyChain360 currently struggles with:
- 📉 Frequent stockouts of high-demand products
- 📦 Overstocking of low-demand inventory
- 🚚 Delayed shipments
- ⏱️ Lack of real-time insights
## 🗂️ Data Fragmentation
Operational data is scattered across multiple systems:
- 🗄️ S3 (CSV/JSON files)
- 🛢️ PostgreSQL transactional database
- 📊 Google Sheets
---
## ⚙️ Tech Stack
| Layer            | Tool           |
| ---------------- | -------------- |
| Orchestration    | Apache Airflow |
| Storage          | AWS S3         |
| Data Warehouse   | Snowflake      |
| Transformation   | dbt            |
| Infrastructure   | Terraform      |
| Containerization | Docker         |
| CI/CD            | GitHub Actions |
---
## 📊 Data Sources
| Source              | Type          | Frequency                     |
| ------------------- | ------------- | ------------------------------|                 
| Product Catalog     | CSV (S3)      | Static dataset (Rarely change)|
| Store Locations     | Google Sheets | Static dataset (Rarely change)|
| Suppliers           | CSV (S3)      | Static dataset (Rarely change)|
| Warehouses          | CSV (S3)      | Static dataset (Rarely change)|
| Inventory Snapshots | CSV (S3)      | Generate Daily                |
| Shipment Logs       | JSON (S3)     | Generate Daily                |
| Sales Transactions  | PostgreSQL    | Generate Daily                |
----
## 🔄 Pipeline Workflow
### Airflow orchestrates:
- Extract data from all sources
- Load raw data into S3 (Parquet format)
- Ingest data into Snowflake (RAW schema)
- Transform data using dbt
- Run data quality checks
- Build analytical models


