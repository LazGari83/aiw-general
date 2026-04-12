# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# MARKDOWN ********************

# # CD022 🔶 Notebook CICD Tricks
# > **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# 8 tricks to help you (and your LLM coding assistant) understand bullet-proof ways to write Fabric Notebooks, so that they can be deployed successfully across DEV, TEST and Production environments. 
# 
# In this tutorial, we'll cover: 
# 1. Write raw files to Lakehouse Files using `notebookutils.fs.put()` with ABFS
# 2. Read JSON from Files and write a delta table using ABFS
# 3. Load a Variable Library to replace hardcoded names
# 4. Parameterized `fs.put()` with Variable Library values
# 5. Parameterized read + delta write with Variable Library values
# 6. Four-part naming in Spark SQL
# 7. Parameterized four-part naming with `spark.sql()` and f-strings
# 8. Delta MERGE using ABFS paths 
# 
# 
# 
# |
# 
# > In Skool, you'll also find the Markdown version of these tricks, which you can feed more efficiently into your coding agent to help them write optimized Fabric notebook code. 


# MARKDOWN ********************

# #### Prerequisites
# 
# 1. Create a Lakehouse (with Schemas) called CD022_Sales in your Fabric Workspace. **Note: It's very important you tick the 'with Schemas' box upon Lakehouse creation, I think it should be the default now**
# 2. Import this notebook into the Workspace. **Don't connect the Lakehouse to the Notebook.**
# 
# 
# #### 1. Write raw JSON to Lakehouse Files via ABFS (using notebookutils.fs)
# 
# ABFS paths are absolute — they specify the workspace and Lakehouse by name (or ID). This negates the need to attach notebooks to a specific Lakehouse which, in my experience, leads to much more predictable deployments. The problem with attaching a Lakehouse to a Notebook is that it creating a dependancy that we don't have full control over, we rely on Microsoft to implement autobinding (and the functionality can change!). Also, autobinding only works with one Lakehouse, so if you have a notebook that reads and writes from TWO lakehouses (quite common), then the autobinding approach won't work. 
# 
# For that reason, and to keep things consistent, I recommend you use the ABFS path as the primary way to read and write data from Lakehouses. The _way_ we do that, for a variety of use cases, is what we'll cover here, starting with writing Files into a Lakehouse Files area (using the ABFS path). 
# 
# For this use case, I recommend the notebookutils file system (fs) module: 


# CELL ********************

# here we're just hardcoding these values, but later on in the notebook, 
# we'll see how to get them from a Variable Library (so that they change when you deploy to a different environment). 

workspace_name = "Fabric_Dojo_2" # change this to your workspace name
lakehouse_name = "CD022_Sales" # change this to your lakehouse name
schema_name = "dbo" # you can leave this as dbo

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json

daily_sales = [
    {"sale_id": 1, "sale_date": "2024-11-01", "product": "Wireless Mouse", "quantity": 5, "unit_price": 29.99, "store_id": "STORE-001"},
    {"sale_id": 2, "sale_date": "2024-11-01", "product": "USB-C Hub", "quantity": 3, "unit_price": 49.99, "store_id": "STORE-002"},
    {"sale_id": 3, "sale_date": "2024-11-02", "product": "Mechanical Keyboard", "quantity": 2, "unit_price": 89.99, "store_id": "STORE-003"},
    {"sale_id": 4, "sale_date": "2024-11-02", "product": "Monitor Stand", "quantity": 8, "unit_price": 39.99, "store_id": "STORE-001"},
    {"sale_id": 5, "sale_date": "2024-11-03", "product": "Desk Lamp", "quantity": 12, "unit_price": 24.99, "store_id": "STORE-004"},
    {"sale_id": 6, "sale_date": "2024-11-03", "product": "Webcam", "quantity": 4, "unit_price": 64.99, "store_id": "STORE-002"},
    {"sale_id": 7, "sale_date": "2024-11-04", "product": "Headset", "quantity": 6, "unit_price": 54.99, "store_id": "STORE-005"},
    {"sale_id": 8, "sale_date": "2024-11-04", "product": "Wireless Mouse", "quantity": 15, "unit_price": 29.99, "store_id": "STORE-003"},
    {"sale_id": 9, "sale_date": "2024-11-05", "product": "USB-C Hub", "quantity": 1, "unit_price": 49.99, "store_id": "STORE-001"},
    {"sale_id": 10, "sale_date": "2024-11-05", "product": "Mechanical Keyboard", "quantity": 7, "unit_price": 89.99, "store_id": "STORE-004"}
]

# create a JSON string from the daily_sales list of dictionaries, with indentation for readability 
json_content = json.dumps(daily_sales, indent=2)

# construct the ABFS path
abfs_files_path = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Files/landing/daily_sales.json"

# write the file out, using the ABFS path and the JSON content we just created. 
# The 'True' argument means that if the file already exists, it will be overwritten. 
notebookutils.fs.put(abfs_files_path, json_content, True)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. Read JSON from Files, write delta table — all via ABFS
# 
# Same ABFS pattern, but targeting `.Lakehouse/Tables/` instead of `.Lakehouse/Files/`. The `multiline` option is needed because our JSON is a pretty-printed array.

# CELL ********************

abfs_source = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Files/landing/daily_sales.json"

# the target path assumed you are using the Lakehouse With Schemas (which is now the default)
abfs_target = f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{schema_name}/daily_sales"

df = spark.read.option("multiline", "true").json(abfs_source)

# importantly, we use .save(), rather than saveAsTable(), so that we can pass in an ABFS path. 
df.write.format("delta").mode("overwrite").save(abfs_target)

df.show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3. Loading from a Variable Library
# 
# In practice, you shouldn't be hard-coding things like Workspace names, Lakehouse Names - we now have the Variable Library for that! 
# 
# Variable Libraries replace hardcoded workspace/Lakehouse names. Each deployment stage (DEV, TEST, PROD) activates its own value set — the notebook code stays the same.
# 
# > Prerequisite: In your workspace (needs to be the same one as your Notebooks, very important!), create a new Variable Library, CD022_Variables. Add the following variables in your Variable Library, and populate them with the same variable values you've been using for the first two tricks. 
# > - WORKSPACE_NAME
# > - LAKEHOUSE_NAME
# > - SCHEMA_NAME
# 
# 


# CELL ********************

vl = notebookutils.variableLibrary.getLibrary("CD022_Variables")

print(f"Workspace: {vl.WORKSPACE_NAME}")
print(f"Lakehouse: {vl.LAKEHOUSE_NAME}")
print(f"Schema:    {vl.SCHEMA_NAME}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 4. Parameterized fs.put()
# 
# Same `fs.put()` as trick 1, but the path is now driven by Variable Library values. This is makes it work across CICD deployment environments (DEV, TEST, PROD). 

# CELL ********************

# construct ABFS path, using variables from Variable Library this time
abfs_files_path = f"abfss://{vl.WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{vl.LAKEHOUSE_NAME}.Lakehouse/Files/landing/daily_sales.json"

notebookutils.fs.put(abfs_files_path, json_content, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 5. Parameterized read + delta write
# 
# Same read-and-write-delta pattern as trick 2, now fully parameterized. Deploy this notebook anywhere — just make sure you remember to change the active value set (in the Variable Library), in the DEV, TEST and PROD workspace. 

# CELL ********************

abfs_source = f"abfss://{vl.WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{vl.LAKEHOUSE_NAME}.Lakehouse/Files/landing/daily_sales.json"
abfs_target = f"abfss://{vl.WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{vl.LAKEHOUSE_NAME}.Lakehouse/Tables/{vl.SCHEMA_NAME}/daily_sales"

df = spark.read.option("multiline", "true").json(abfs_source)

df.write.format("delta").mode("overwrite").save(abfs_target)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 6. Four-part naming in Spark SQL
# 
# One of the major unlocks with the new Lakehouse (with Schemas enabled); you can query any table in any workspace using the four-part naming system: `workspace.lakehouse.schema.table`. 
# 
# If your workspace contains special characters, you should wrap it in backticks `like_this` 

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM `Fabric_Dojo_2`.`CD022_Sales`.`dbo`.`daily_sales` LIMIT 10

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 7. Parameterized four-part naming with spark.sql()
# 
# `%%sql` cells can't use variables. Use `spark.sql()` with f-strings instead — this is where Variable Library + four-part naming + parameterization all come together.

# CELL ********************

result = spark.sql(f"""
    SELECT product, COUNT(*) as sales, SUM(quantity) as qty, ROUND(SUM(quantity * unit_price), 2) as revenue
    FROM `{vl.WORKSPACE_NAME}`.`{vl.LAKEHOUSE_NAME}`.{vl.SCHEMA_NAME}.daily_sales
    GROUP BY product
    ORDER BY revenue DESC
""")

display(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 8. Delta MERGE with DeltaTable.forPath() and ABFS
# 
# We can use Spark SQL MERGE using the same PySpark spark.sql(""" """) syntax that we saw in trick 7. 
# 
# Another method we can use for merging tables in Fabric Spark is using the DeltaTable python library. Here's how to make that library work with ABFS paths. 
# 
# `DeltaTable.forPath()` accepts an ABFS path — so you can MERGE into any table in any workspace without a default Lakehouse. This is the pattern used in the Intermediate Real-World Project for incremental loading.

# CELL ********************

from delta.tables import DeltaTable

abfs_table_path = f"abfss://{vl.WORKSPACE_NAME}@onelake.dfs.fabric.microsoft.com/{vl.LAKEHOUSE_NAME}.Lakehouse/Tables/{vl.SCHEMA_NAME}/daily_sales"

updates = spark.createDataFrame([
    (1, "2024-11-01", "Wireless Mouse", 10, 29.99, "STORE-001"),
    (99, "2024-11-06", "USB-C Dock", 3, 79.99, "STORE-003")
], ["sale_id", "sale_date", "product", "quantity", "unit_price", "store_id"])

delta_table = DeltaTable.forPath(spark, abfs_table_path)

(
    delta_table.alias("target")
    .merge(updates.alias("source"), "target.sale_id = source.sale_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

spark.read.format("delta").load(abfs_table_path).orderBy("sale_id").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# 
# ## Pass Criteria
# 
# You will pass this exercise when you achieved the following:
# 
# - ✅ Your notebook writes raw JSON data to Lakehouse Files and reads it back to create a delta table in Lakehouse Tables, using ABFS paths and Variable Library values — no hardcoded workspace or Lakehouse names in the code
# 
# - ✅ You can query your table using parameterized four-part naming in Spark SQL: {vl.Workspace_Name}.{vl.Lakehouse_Name}.{vl.Schema_Name}.daily_sales — and the query returns results
# 
# - ✅ Your Variable Library has at least two value sets (Default and PROD), and you understand that changing the active value set would redirect all your notebook's data operations to a different environment without changing a line of code
# 
# ## Summary 
# I hope you found that helpful - I hope it gives you some ideas for writing your notebooks in a way that gives you less CI/CD headaches! 
# 
# Now, head [back to Skool](https://www.skool.com/fabricdojo/classroom/486e6c52?md=b93082407f3f4968904d3a88a6b2c66e) for the Reflection. 

