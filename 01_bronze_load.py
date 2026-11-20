# Bronze layer: land the raw file as a Delta table. No cleaning here.
# Input: CSV uploaded to a Databricks Volume (output of ingest_ems_data.py)

df_full = spark.read.csv(
    "/Volumes/ems_pipeline/bronze/ems_files/ems_incident_dispatch_2025_raw.csv",
    header=True,
    inferSchema=True,
)

df_full.write.mode("overwrite").saveAsTable(
    "ems_pipeline.bronze.bronze_ems_incidents_2025"
)

# Check: should match the row count printed by the ingestion script (1,612,273)
print(df_full.count())
