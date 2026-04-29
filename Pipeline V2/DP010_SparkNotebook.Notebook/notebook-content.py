# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2db8d35a-1dd7-42a4-8e8b-8fcb1919e881",
# META       "default_lakehouse_name": "DP010_Lakehouse",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "2db8d35a-1dd7-42a4-8e8b-8fcb1919e881"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## DP010 🟢 Implement 3 incremental loading methods 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 


# MARKDOWN ********************

# ## Prerequisites
# - In your Fabric Workspace, create a new Lakehouse (without Schemas) called DP010_Lakehouse. 
# - Download the datasets ZIP file, unzip the folder, and then upload the Folder into the Files area of your Lakehouse. 
# - Download this Spark Notebook (if you haven't already) from the bottom of this page, import it into the Workspace, connect it to the Lakehouse. 
# - **Run the code snippet below to load the Starter CSV dataset into the `sales_figures` table for the first time.**

# CELL ********************

df = (
    spark.read.format("csv")
    .option("header","true")
    .option("inferSchema","true")
    .load("Files/data.csv")
)
df.write.mode("overwrite").format("delta").saveAsTable('sales_figures')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Background - what we're actually doing here
# Ok, so now you should have a single table in your Lakehouse: `sales_figures`. The goal of this exercise is to incrementally update that `sales`figures` table using three different methods: 
# - Spark Merge (Task 1)
# - Copy Job Upsert (Task 2) 
# - Data Pipeline Copy Data Activity (Task 3)
# 
# For each method, you will find a different CSV file in `Files/1_upserts/`. The idea is that you will proressively update the `sales_figures` table, and at the end, you should finish with one table that has been updated by many different techniques. 
# 
# Let's begin. 


# MARKDOWN ********************

# ## Task 1: Using Delta/ Spark
# Let's start with a recap of the Spark MERGE functionality - here we'll be using PySpark to load the CSV, then Spark SQL for the MERGE. You can read more about Spark SQL MERGE syntax, [here](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge) 

# CELL ********************

# load the spark upserts
spark_upserts_csv = (
    spark.read.format("csv") 
    .option("header", True) 
    .option("inferSchema", True) 
    .load("Files/spark_upserts.csv")
)

# write to a view, so that we can use Spark SQL
spark_upserts_csv.createOrReplaceTempView("spark_upserts")
display(spark_upserts_csv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Next, use Spark SQL to MERGE the `sales_figures` table with the `spark_upserts` view, on the `row_id`:

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO `sales_figures` AS tgt
# MAGIC USING `spark_upserts` AS src
# MAGIC ON tgt.row_id = src.row_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# You can check the result in with this code snippet:

# CELL ********************

display(spark.table('sales_figures'))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tasks 2 and 3
# After Task 1, go back to the Skool tutorial page where you'll find the instructions for Tasks 2 and 3!

