# Fabric notebook source


# MARKDOWN ********************

# # AG-LAK-010-Wayfarer -- release 2
# Adds `carrier` to the live `voyage.leg` table via a schema-evolving overwrite.

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# LH-C10: explicit schema, never inferred. Release 2 adds `carrier`.
schema = StructType([
    StructField("leg_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("carrier", StringType(), True),
])

df = spark.read.option("header", True).schema(schema).csv("Files/raw/leg.csv")

spark.sql("CREATE SCHEMA IF NOT EXISTS voyage")

# LH-C17: evolve the LIVE table -- overwriteSchema=true is additive-safe for a new nullable
# column and keeps Delta history (version 0 survives via time travel). The release-2 extract
# is a full snapshot (all legs, every row already carrying `carrier`), so a whole-table
# overwrite from it leaves no row with a null carrier -- an append-only evolution would not.
(df.write.format("delta").mode("overwrite")
   .option("overwriteSchema", "true")
   .saveAsTable("voyage.leg"))

print(f"release 2: wrote {df.count()} row(s) to voyage.leg (carrier added)")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
