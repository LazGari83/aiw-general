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

# Read the CSV data into a Spark Dataframe
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("Files/healthcare_noshows.csv")

# Write the contents of the Spark dataframe into your Lakehouse table (overwriting) 
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save("Tables/healthcare_full")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
