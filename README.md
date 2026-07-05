# 🇩🇪 German Energy Intelligence Platform

**End-to-end energy data platform built on official Bundesnetzagentur SMARD data — real-time PySpark ingestion, Medallion architecture across BigQuery and Snowflake with dbt, Prophet forecasting per German TSO region, anomaly detection on grid behaviour, and a Groq LLaMA agent answering natural-language questions grounded in live data.**

🔗 **[Live Demo](https://smard-energy-pipeline.streamlit.app)** &nbsp;|&nbsp; 📁 **[GitHub](https://github.com/OJASIO/smard-energy-pipeline)**

---

## What it does

Germany's electricity grid generates quarter-hourly generation, demand, and cross-border flow data through the Bundesnetzagentur's SMARD platform. This project ingests that data in real time, cleans and transforms it through a Medallion architecture, trains forecasting and anomaly-detection models on the Gold layer, and exposes everything through a bilingual (German/English) intelligence agent with a live dashboard.

A hiring manager at TransnetBW or EnBW can open the live URL, ask "Wie war der Anteil erneuerbarer Energien letzten Monat?" and receive a data-grounded answer in German within 2–3 seconds.

---

## Architecture

📐 **[Full Architecture Diagram (PDF)](https://github.com/OJASIO/smard-energy-pipeline/blob/main/SMARD_Pipeline_Architecture.pdf)**

*Lambda Architecture + ELT + Medallion (Bronze/Silver/Gold) — orchestrated by Apache Airflow*

```
SMARD API (Bundesnetzagentur)
        │
        ▼
┌─────────────────────────────────────┐
│  BRONZE — Google BigQuery           │
│  Raw ingestion via PySpark          │
│  Energy · Weather · Cross-border    │
│  Great Expectations validation      │
└──────────────┬──────────────────────┘
               │ PySpark batch + stream
               ▼
┌─────────────────────────────────────┐
│  SILVER — Snowflake                 │
│  Cleaned + unified via dbt          │
│  stg_energy_unified                 │
│  stg_weather_unified                │
│  stg_cross_border_clean             │
│  Source freshness monitoring        │
└──────────────┬──────────────────────┘
               │ dbt incremental models
               ▼
┌─────────────────────────────────────┐
│  GOLD — Snowflake                   │
│  fct_energy_readings                │
│  agg_regional_comparison            │
│  agg_daily_generation               │
│  RENEWABLE_FORECAST (Prophet)       │
│  ANOMALY_FLAGS (Prophet CI)         │
└──────────────┬──────────────────────┘
               │
       ┌───────┴───────┐
       ▼               ▼
  LLM Agent      Streamlit Dashboard
  Groq LLaMA     Live URL
  7-layer         smard-energy-pipeline
  security        .streamlit.app
```

**Orchestration:** Apache Airflow on GCP VM
- `smard_stream_monitor` — every 15 min: poll → Bronze → Silver → Gold
- `smard_daily_pipeline` — daily 01:00: validate → freshness check → Silver → Gold → test

---

## Results

| Component | Metric | Result |
|---|---|---|
| **Forecasting** (Prophet, national DE) | Holdout MAE | 28,147 MWh/day |
| **Forecasting** | Holdout MAPE | 38.98% (7-day holdout) |
| **Forecasting** | Training history | 2017–2026 (9 years) |
| **Anomaly Detection** | Flagged (last 90d) | 9 events |
| **Anomaly Detection** | Manual validation | 6/6 confirmed German public holidays |
| **Anomaly Detection** | False positives | 0 (vs 74 with z-score baseline) |
| **LLM Agent** | Context Precision (RAGAS) | 1.0 |
| **LLM Agent** | Faithfulness (RAGAS) | 0.55 |
| **LLM Agent** | Security effectiveness | 3/3 (100%) |
| **LLM Agent** | Avg response latency | ~2,233 ms |
| **Pipeline** | dbt source freshness | 4/5 PASS, 1 WARN (cross-border expected) |

---

## Key Technical Highlights

**Data quality debugging under real conditions.** During development, the pipeline was silently building Gold from `SMARD_DEV` instead of the real Silver layer — a source isolation bug traced through dbt's `{{ target.name }}` configuration. Separately, a Great Expectations validation threshold (`±50,000 MW`) was incorrectly scoped for bilateral cross-border flows rather than Germany's national `physical_total` aggregate, causing 6 days of silent pipeline failure. Both were identified by tracing task logs rather than accepting green Airflow status indicators at face value.

**Prophet-based anomaly detection beats z-score on energy data.** A rolling z-score detector (2-day window) produced 74 false positives driven by weekday-vs-weekend demand baseline differences — Sunday morning demand patterns looked anomalous relative to Friday/Saturday baselines. Switching to Prophet confidence intervals (95%) reduced flags to 9, all confirmed real events: 6 German public holidays with characteristic demand drops (Easter Sunday demand: 1,034,056 MWh actual vs 1,313,139 MWh baseline, −21%), 1 renewable spike, 1 partial-day artifact.

**Production-grade LLM security.** The agent implements 7 defence layers: IAM least-privilege (read-only Snowflake role scoped to 4 Gold tables), input validation against prompt injection and SQL injection patterns, hardened system prompt, output filtering for credential leakage, append-only audit logging, environment-variable-only secrets management, and session rate limiting. For production: Azure OpenAI (Germany West Central region) for DSGVO compliance; architecture is provider-agnostic.

**Bilingual German/English support.** `langdetect` routes questions to the LLM with language-specific instructions. LLaMA 3.3 70B handles German natively. Input validator extended with German energy vocabulary. Example: "Wie war der Anteil erneuerbarer Energien letzten Monat?" returns a German-language answer grounded in live SMARD data.

**Pipeline self-healing architecture.** The stream monitor (`*/15 * * * *`) now runs `dbt run --select gold` after every Silver write, eliminating Gold staleness. The daily pipeline runs `dbt source freshness` before transformations — if Silver is stale (>2 days for historical, >6 hours for stream), the pipeline fails explicitly rather than building Gold on stale data. Incremental lookback widened from 3 to 7 days to handle realistic pipeline recovery gaps.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | PySpark, Python, SMARD API, Open-Meteo API |
| Bronze | Google BigQuery, Google Cloud Storage |
| Transformation | Apache Spark (batch + streaming) |
| Silver/Gold | Snowflake, dbt (incremental + merge strategies) |
| Data Quality | Great Expectations, dbt source freshness |
| Orchestration | Apache Airflow (GCP VM, europe-west3) |
| Forecasting | Facebook Prophet (weekly + yearly seasonality) |
| Anomaly Detection | Prophet confidence intervals (95%) |
| LLM Agent | Groq LLaMA 3.3 70B, direct Groq SDK |
| Agent Evaluation | Custom RAGAS-equivalent (Faithfulness, Context Precision, Context Recall) |
| Dashboard | Streamlit, Plotly |
| Deployment | Streamlit Cloud |
| Language Detection | langdetect (DE/EN routing) |

**Production equivalent:** Azure OpenAI (Germany West Central) + Azure Key Vault + Azure Monitor for BSI critical infrastructure compliance and DSGVO data residency.

---

## Agent Query Templates

The LLM agent answers natural-language questions (German or English) using 6 fixed query templates grounded in live Gold-layer data:

| Template | Example questions |
|---|---|
| `FORECAST` | "What is the renewable energy forecast for the next 7 days?" |
| `ANOMALIES` | "Were there any unusual grid events recently?" |
| `RENEWABLE_SHARE` | "Wie war der Anteil erneuerbarer Energien letzten Monat?" |
| `DEMAND` | "How has electricity demand changed recently?" |
| `GENERATION` | "Welche Energiequelle hat zuletzt am meisten Strom erzeugt?" |
| `COMPARISON` | "How does renewable generation compare to last year?" |

Fixed templates were chosen over text-to-SQL for reliability in a 7-day build timeline. Full text-to-SQL agent identified as next step.

---

## Known Limitations and Next Steps

**National-level only.** Gold layer aggregates at DE national grain. Per-TSO split (50Hertz / Amprion / TenneT / TransnetBW) exists in the raw SMARD data but was not ingested at this granularity — identified as a pipeline extension for post-sprint work.

**SMARD publication lag.** SMARD publishes validated quarter-hourly data with a 5–7 day lag. The dashboard reflects the latest published data, not real-time grid state.

**Forecasting accuracy.** Prophet MAPE of 38.98% on the 7-day holdout is inflated by the specific holdout window; MAE of 28,147 MWh/day is the more stable metric. LSTM/Temporal Fusion Transformer comparison deferred to post-fast.ai work (~August).

**Next steps (ranked by value):**
1. Per-TSO forecasting once regional ingestion is extended
2. Isolation Forest anomaly detection with SHAP explainability
3. Full text-to-SQL agent with LangGraph orchestration
4. Azure OpenAI deployment for DSGVO compliance

---

## Running Locally

```bash
git clone https://github.com/OJASIO/smard-energy-pipeline
cd smard-energy-pipeline
pip install -r requirements.txt

export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export GROQ_API_KEY=your_groq_key

streamlit run app.py
```

---

## Data Source

All energy data sourced from **[SMARD](https://www.smard.de)** (Strommarktdaten), the official electricity market data platform of the **Bundesnetzagentur** (Federal Network Agency of Germany). Data is publicly available under open data principles.

Weather data: Open-Meteo ERA5 reanalysis archive.
