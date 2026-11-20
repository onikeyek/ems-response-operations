# Silver layer: fix types, standardize labels, derive shift.
# Rows are NOT dropped here. Validity flags are handled in Gold.
from pyspark.sql.functions import when, hour

df = spark.table("ems_pipeline.bronze.bronze_ems_incidents_2025")

# 1. Identifier columns were inferred as double. They are labels, not quantities.
id_columns = [
    "zipcode", "policeprecinct", "citycouncildistrict",
    "communitydistrict", "communityschooldistrict", "congressionaldistrict",
]
for c in id_columns:
    df = df.withColumn(c, df[c].cast("string"))

# 2. Standardize the borough label
df = df.withColumn(
    "borough",
    when(df["borough"] == "RICHMOND / STATEN ISLAND", "STATEN ISLAND")
    .otherwise(df["borough"]),
)

# 3. Derive shift from the hour of the call (no native shift field exists)
df = df.withColumn(
    "shift",
    when((hour("incident_datetime") >= 6) & (hour("incident_datetime") < 14), "Day")
    .when((hour("incident_datetime") >= 14) & (hour("incident_datetime") < 22), "Evening")
    .otherwise("Night"),
)

df.write.mode("overwrite").saveAsTable("ems_pipeline.silver.silver_ems_incidents_2025")
print(df.count())  # should still be 1,612,273
