# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7cacb0b4-d28f-47b0-a248-2c1de03a8c79",
# META       "default_lakehouse_name": "LH001_Fundamentals",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "7cacb0b4-d28f-47b0-a248-2c1de03a8c79"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE healthcare_sql2 (
# MAGIC     `PatientId` INT,
# MAGIC     `Gender` STRING,
# MAGIC     `Scholarship` BOOLEAN
# MAGIC )


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

# Define the Delta table schema
healthcare_reduced_schema = StructType([
    StructField("PatientId", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Scholarship", BooleanType(), True)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import DeltaTable

# Create Delta table with the defined schema
DeltaTable.create(spark) \
    .tableName("healthcare_delta") \
    .addColumns(healthcare_reduced_schema) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

updated_healthcare_schema = StructType([
    StructField("PatientId", IntegerType(), True),
    StructField("Gender", StringType(), True),
    StructField("Scholarship", BooleanType(), True),
    StructField("Age", IntegerType(), True)
])


# Update the Delta table schema using createOrReplace
DeltaTable.createOrReplace(spark) \
    .tableName("healthcare_delta") \
    .addColumns(updated_healthcare_schema) \
    .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
    spark.read.format("csv")
        .option("header","true")
        .schema(updated_healthcare_schema)
        .load("abfss://304b1974-676d-48af-b4e4-8cf6f0fe139e@onelake.dfs.fabric.microsoft.com/7cacb0b4-d28f-47b0-a248-2c1de03a8c79/Files/initial_healthcare_data.csv")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView("initial_healthcare_load")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO healthcare_delta AS target
# MAGIC USING initial_healthcare_load AS source
# MAGIC ON target.PatientId = source.PatientId
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_view = spark.table('healthcare_delta')
display(df_view)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# get our hc delta table
healthcare_delta = DeltaTable.forName(spark,"healthcare_delta")

# create a DF of the updates (from Lakehouse Files area)
dfUpdates = (
    spark.read.format("csv")
        .option("header","true")
        .schema(updated_healthcare_schema)
        .load("abfss://304b1974-676d-48af-b4e4-8cf6f0fe139e@onelake.dfs.fabric.microsoft.com/7cacb0b4-d28f-47b0-a248-2c1de03a8c79/Files/update_healthcare_data.csv")
)

# perform the merge
(
healthcare_delta
    .alias("target")
    .merge(dfUpdates.alias("source"),"target.PatientId = source.PatientId")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll() 
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_view = spark.table('healthcare_delta')
display(df_view)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
