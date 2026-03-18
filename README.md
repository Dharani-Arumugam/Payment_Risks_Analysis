# Payment_Risks_Analysis
An end-to-end data engineering project simulating a real-world payments system using Databricks, AWS S3, Auto Loader, and Delta Lake. The pipeline ingests synthetic transaction data and builds a Bronze–Silver–Gold lakehouse architecture to enable fraud detection and business analytics.

Designed and implemented a scalable data pipeline using Databricks Auto Loader and Delta Lake to process 2K+ simulated payment transactions, enriching data with cross-border, risk-tier, and payment-method signals in the Silver layer.

``
Python Data Generator
    ↓
AWS S3 (Raw JSON - NDJSON)
    ↓
Databricks Auto Loader (cloudFiles)
    ↓
Bronze Layer (Raw Delta Tables)
    ↓
Silver Layer (Enriched + Joined Data)
    ↓
Gold Layer (Fraud Signals + Business KPIs)
    ↓
Dashboard / Analytics
``


🧱 Data Model
  - Dimensions
  - users
  - merchants
  - payment_methods

Fact Tables
  - transactions
  - refunds
  - chargebacks

🥉 Bronze Layer

  -  Ingested using Auto Loader (cloudFiles)
  -  Stored as Delta tables
  -  Includes metadata columns:
        - _ingest_ts
        - _file_name
   
🥈 Silver Layer

Main table:
  - payments.silver.transactions_enriched
Features created:
  - Cross-border flags (user vs txn, method vs txn)
  - Payment method activity (active/inactive)
  - High amount transactions
  - API channel detection
  - Merchant & user risk enrichment
  - Refund & chargeback labels

🥇 Gold Layer
1. payment_funnel
    - Completion rate
    - Failure rate
    - Refund rate
    - Chargeback rate
    - Aggregated by channel, network, country

2. fraud_signals
    - Rule-based fraud detection
    - Risk scoring model

3. Action classification:
     - allow
     - review
     - block
  
📊 Dashboard Insights

  -  Transaction success vs failure distribution
  -  Completion rate by channel (web/mobile/api)
  -  Refund rates by payment network
  -  Chargebacks by geography
  -  Fraud action distribution
  -  Top triggered fraud rules

🧠 Key Highlights
  - Built a Medallion Architecture (Bronze → Silver → Gold)
  - Used Auto Loader for scalable ingestion
  - Designed realistic payment simulation (refunds, chargebacks, fraud signals)
  - Implemented rule-based fraud scoring system
  - Created analytics-ready Gold layer for business insights
