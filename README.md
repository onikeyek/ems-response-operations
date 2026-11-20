# NYC EMS Response Operations: Dispatch Delay Analysis

An end-to-end data project on **1.6 million real NYC EMS incidents (2025)**. I built a
Bronze/Silver/Gold pipeline in Databricks and a live Power BI dashboard to answer one
operational question:

> Are there boroughs or shifts where EMS response times consistently miss the 8-minute
> target, and is the bottleneck **dispatch delay** or **drive time**?

## Key finding

Drive time is similar across NYC. **Dispatch delay is not.**

| Borough (Evening shift) | Avg. dispatch delay | Avg. drive time | % meeting 8-min benchmark |
|---|---|---|---|
| Bronx | 692.4 s | 688.3 s | 29.3% |
| Manhattan | 629.4 s | 667.9 s | 34.0% |
| Queens | 278.3 s | 699.0 s | 32.8% |
| Brooklyn | 238.5 s | 649.4 s | 38.2% |
| Staten Island | 121.9 s | 565.0 s | 43.3% |

- Across all validated 2025 incidents, **38.7%** met the 8-minute benchmark.
- Bronx and Manhattan Evening shifts have the longest dispatch delays and the lowest
  pass rates. Drive time stays in a narrow band (about 565-699 s) in every borough.
- Incident volume does not explain it: Brooklyn handles more incidents than the Bronx
  and still performs better.
- This points to a **dispatch-side staffing or process gap** in specific boroughs and
  shifts, not a citywide fleet or geography problem.

![Dashboard overview](images/dashboard_overview.png)

## Architecture

```
Socrata API (NYC Open Data)
        |
   Python ingestion (requests, paginated)
        |
   Bronze  - raw data, untouched (Delta table)
        |
   Silver  - typed, standardized, shift derived (Delta table)
        |
   Gold    - KPI and borough/shift summary tables (Delta table)
        |
   Databricks SQL Warehouse --> Power BI (Import mode)
```

## Tech stack

Python, PySpark, Delta Lake, Databricks (free tier), Socrata SODA API, SQL Warehouse,
Power BI Desktop.

## Data

- **Source:** NYC Open Data, *EMS Incident Dispatch Data* (FDNY CAD system), dataset ID `76xm-jjuj`
- **Scope:** all of 2025, 1,612,273 incidents
- **Stage timestamps used:** call received, dispatched, unit responding, on scene, incident closed
- The raw CSV is not in this repo (about 308 MB). Run `ingest_ems_data.py` to recreate it.

## Repository structure

```
.
├── README.md
├── ingest_ems_data.py              # Socrata API ingestion, paginated
├── databricks/
│   ├── 01_bronze_load.py           # CSV in a Volume -> Bronze Delta table
│   ├── 02_silver_transform.py      # type fixes, borough cleanup, shift
│   └── 03_gold_aggregation.py      # durations, benchmark flag, summaries
├── docs/
│   └── EMS_Dashboard_Export.pdf    # dashboard export
└── images/
    └── dashboard_overview.png
```

## Data quality decisions

1. **Schema drift.** My early research assumed hospital-transport timestamps existed.
   Checking `df.columns` on the live data showed they do not, so I scoped the analysis
   to the five stages that are present. The core question (response time) was unaffected.
2. **Wrong data types.** Zip code, precinct and four district columns were inferred as
   `double`. They are identifiers, not quantities, so Silver casts them to `string`.
3. **Inconsistent labels.** `RICHMOND / STATEN ISLAND` was standardized to `STATEN ISLAND`.
4. **Untrustworthy timing rows.** About 2-6% of rows carry a validity flag of `N`
   (mostly standby units, with impossible values such as negative drive times).
   I kept them in Silver and only excluded them from timing KPIs in Gold, so incident
   volume counts stay complete.
5. **Weighted average.** The overall pass rate is computed as total met / total valid
   incidents. Averaging the 15 borough/shift percentages would have given a small
   borough the same weight as a large one.
6. **Dev sample first.** I built and checked the whole pipeline on January 2025
   (137,643 rows), then re-ran the same logic on the full year.
7. **Small residual group.** Three incidents have borough `UNKNOWN`. They are excluded
   from the dashboard charts and noted here rather than guessed.

## How to run

1. Create a free app token at data.cityofnewyork.us (Developer Settings).
2. Set it as an environment variable. Never commit it:
   `export NYC_APP_TOKEN="your_token_here"`
3. Run `python ingest_ems_data.py` to download the 2025 data.
4. Upload the CSV to a Databricks Volume, then run the three notebooks in `databricks/`
   in order.
5. Connect Power BI to the SQL Warehouse and load the two Gold tables.

## Limitations and next steps

- The 8-minute benchmark is a common urban EMS target, not an official FDNY target.
- The dashboard shows where and when delays happen. It does not prove why. Dispatch
  staffing data would be needed to test the staffing explanation directly.
- Next: schedule the pipeline as a Databricks job, and add month-by-month trends.

## Author

Dolapo (Naimot Yekini) - [GitHub](https://github.com/onikeyek) -
[Portfolio](https://onikeyek.github.io/NaimotYekini.github.io/)
