# IoT Predictive Maintenance Pipeline

An end-to-end Azure data engineering project: batch and streaming ingestion, a medallion architecture (Bronze/Silver/Gold), real-time windowed aggregation, and a Power BI dashboard — built to catch early signs of equipment failure before it happens.

## The Problem

Reactive maintenance — waiting for a machine to fail, then fixing it — is expensive and disruptive. This project simulates a predictive-maintenance approach: continuously comparing each machine's recent sensor readings (temperature, vibration, pressure) against its own healthy baseline, flagging early drift before failure occurs.

## Architecture

<!-- Add architecture diagram screenshot here -->

**Batch pipeline:** Bronze (raw CSV landing) → Silver (Azure Data Factory Data Flows — typed, cleaned) → Gold (Azure Data Factory — rolling averages, baseline comparison, health flagging) → Power BI

**Streaming pipeline** (parallel, independent demo): a Python script simulates real-time sensor events → Azure Event Hub → Azure Stream Analytics (30-second tumbling window aggregation, per machine) → Data Lake output

## Key Design Decisions

- **Azure Data Factory Data Flows, not PySpark/Databricks** — chosen deliberately for a defensible, hands-on-verified build rather than a tool learned only in theory
- **Baseline computed from only the first 15 days per machine** — using each machine's known-healthy period as the reference, rather than its full history (see Debugging Notes below for why this matters)
- **Streaming kept to lightweight windowed aggregation, not the full predictive model** — the baseline-comparison logic needs complete historical context that a true, unbounded stream doesn't have, so it stays in batch; streaming demonstrates a different, complementary capability (immediate signals within a moving time window)
- **Short, deliberate Stream Analytics runs** — started, used for a brief burst, and stopped immediately, since it's the one component billed per hour regardless of data volume

## Debugging Notes (a real engineering story, not a polished demo)

Building this pipeline surfaced three genuine, non-obvious bugs — found and fixed using Azure's own pipeline-run diagnostic JSON, not guesswork:

1. **Baseline contamination**: an early version computed each machine's baseline as an average over its *entire* history — which quietly absorbed the very drift it was meant to detect against, since later degraded readings pulled the "normal" average upward. Fixed by restricting the baseline calculation to a Filter (first 15 days) → Aggregate (collapsed to one row per machine), rather than a Window transformation, which keeps every row instead of summarizing.

2. **Silver layer row duplication**: a Sink dataset lacked "Clear the folder," causing successive pipeline reruns to silently accumulate duplicate files rather than replace them — inflating 2,880 clean rows to 2,978+ contaminated ones. Confirmed via the `rowsRead` field in the pipeline's run diagnostics, comparing the original clean run against a later contaminated one.

3. **Silent Window misconfiguration**: the rolling-average Window transformation's "Range by" offset displayed correctly in the UI but wasn't actually taking effect — the computed rolling average was silently equal to each row's own raw value (a window size of 1, not 4). Confirmed by checking whether `rolling_avg_temp == temperature_c` across all rows (it did, 100% of the time) — re-entering the offset settings resolved it.

All three were independently verified against a parallel reference implementation built in pandas, until ADF's output matched to floating-point precision.

## Tech Stack

- **Storage**: Azure Data Lake Storage Gen2 (hierarchical namespace)
- **Batch orchestration & transformation**: Azure Data Factory (Pipelines + Data Flows)
- **Streaming**: Azure Event Hubs, Azure Stream Analytics
- **Visualization**: Power BI Desktop
- **Local prototyping/validation**: Python (pandas), used to independently verify pipeline logic before and after ADF debugging

## Repository Structure

```
├── notebooks/
│   └── local_validation.ipynb          # pandas reference implementation used to verify ADF output
├── scripts/
│   └── send_sensor_events.py           # simulates real-time sensor data into Event Hub
├── powerbi/
│   └── predictive_maintenance.pbix
├── docs/
│   ├── architecture_diagram.png
│   └── screenshots/
├── data/
│   └── sample/                          # small sample of synthetic source data
└── README.md
```

## Notes on Data

All data is synthetic, generated locally with `pandas`/`numpy`: 12 machines, 60 days of sensor readings (temperature, vibration, pressure) at 4 readings/day, with a subset of machines given a deliberate degradation trend to validate the health-flagging logic against a known ground truth.
