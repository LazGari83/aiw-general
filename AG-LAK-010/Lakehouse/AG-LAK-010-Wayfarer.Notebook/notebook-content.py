# Fabric notebook source


# MARKDOWN ********************

# # AG-LAK-010-Wayfarer -- release 1
# Loads the attached lakehouse's `Files/raw/leg.csv` into `voyage.leg` as the table's first version.

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# LH-C10: explicit schema, never inferred -- types are a contract, not a guess.
schema = StructType([
    StructField("leg_id", StringType(), True),
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("distance_km", DoubleType(), True),
])

# LH-C06/LH-C21: relative path against the ATTACHED default lakehouse -- the same source
# runs unchanged in every environment; which environment's bytes land here is decided at
# run submission (the Jobs-API default-lakehouse attachment), never by a name in this cell.
df = spark.read.option("header", True).schema(schema).csv("Files/raw/leg.csv")

# LH-C02: explicit schema.table -- an unqualified write lands in dbo.
spark.sql("CREATE SCHEMA IF NOT EXISTS voyage")

# Release 1: the table's FIRST version, in a single write.
df.write.format("delta").mode("overwrite").saveAsTable("voyage.leg")

print(f"release 1: wrote {df.count()} row(s) to voyage.leg")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
