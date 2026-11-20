# Gold layer: stage durations, benchmark flag, borough/shift summary, KPI table.
from pyspark.sql import Row
from pyspark.sql.functions import (
    col, unix_timestamp, when, count, avg, round as spark_round, sum as spark_sum
)

df = spark.table("ems_pipeline.silver.silver_ems_incidents_2025")

# 1. Stage durations in seconds
df_gold = (
    df.withColumn("dispatch_delay_seconds",
                  unix_timestamp("first_assignment_datetime") - unix_timestamp("incident_datetime"))
      .withColumn("drive_time_seconds",
                  unix_timestamp("first_on_scene_datetime") - unix_timestamp("first_activation_datetime"))
      .withColumn("total_response_seconds",
                  unix_timestamp("first_on_scene_datetime") - unix_timestamp("incident_datetime"))
)

# 2. Which stage took longer for each incident
df_gold = df_gold.withColumn(
    "bottleneck_stage",
    when(df_gold["dispatch_delay_seconds"] >= df_gold["drive_time_seconds"], "Dispatch Delay")
    .otherwise("Drive Time"),
)

# 3. 8-minute (480 s) benchmark flag
df_gold = df_gold.withColumn(
    "meets_benchmark",
    when(df_gold["total_response_seconds"] <= 480, "Y").otherwise("N"),
)

# 4. Timing KPIs use only rows the source marks as valid.
#    (All rows are kept in Silver so volume counts stay complete.)
df_valid = df_gold.filter(df_gold["valid_incident_rspns_time_indc"] == "Y")

# 5. Borough x shift summary: the table Power BI reads
gold_summary = (
    df_valid.groupBy("borough", "shift").agg(
        count("incident_id").alias("incident_volume"),
        spark_round(avg("total_response_seconds"), 1).alias("avg_response_seconds"),
        spark_round(avg("dispatch_delay_seconds"), 1).alias("avg_dispatch_delay_seconds"),
        spark_round(avg("drive_time_seconds"), 1).alias("avg_drive_time_seconds"),
        spark_sum(when(df_valid["meets_benchmark"] == "Y", 1).otherwise(0)).alias("met_benchmark_count"),
    ).withColumn(
        "pct_meeting_benchmark",
        spark_round(col("met_benchmark_count") / col("incident_volume") * 100, 1),
    )
)
gold_summary.write.mode("overwrite").saveAsTable(
    "ems_pipeline.gold.gold_ems_response_summary_2025"
)

# 6. KPI cards. The overall rate is weighted (total met / total valid),
#    not an average of the 15 group percentages.
total_incidents = df.count()
totals = gold_summary.agg(
    spark_sum("incident_volume").alias("valid"),
    spark_sum("met_benchmark_count").alias("met"),
).collect()[0]
overall_pct = round(totals["met"] / totals["valid"] * 100, 1)

# Exclude the tiny UNKNOWN borough (3 incidents) from best/worst
ranked = gold_summary.filter(col("borough") != "UNKNOWN")
worst = ranked.orderBy("pct_meeting_benchmark").first()
best = ranked.orderBy("pct_meeting_benchmark", ascending=False).first()

kpi_summary = spark.createDataFrame([
    Row(metric="Total Incidents Analyzed", value=f"{total_incidents:,}"),
    Row(metric="Overall % Meeting Benchmark", value=f"{overall_pct}%"),
    Row(metric="Worst Performer",
        value=f"{worst['borough'].title()} {worst['shift']} ({worst['pct_meeting_benchmark']}%)"),
    Row(metric="Best Performer",
        value=f"{best['borough'].title()} {best['shift']} ({best['pct_meeting_benchmark']}%)"),
])
kpi_summary.write.mode("overwrite").saveAsTable("ems_pipeline.gold.gold_kpi_summary_2025")
kpi_summary.show(truncate=False)
