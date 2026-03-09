# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4575ff2e-8dc7-49ad-87b2-d143d7d38bae",
# META       "default_lakehouse_name": "LH001_Fundamentals",
# META       "default_lakehouse_workspace_id": "24abee62-a042-437f-8ad6-d8b5d8fe5d4b"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # LH002 🟢 Delta Lake format
# #### Prerequisites: 
# 
# 1. This tutorial assumes you have a Lakehouse in your workspace - you can use the same one from the previous exercise, LH001_Lakehouse. 
# 2. Connect this notebook to that Lakehouse. 
# 3. It's recommended (but optional) that you download the [OneLake File Explorer](https://www.microsoft.com/en-us/download/details.aspx?id=105222) desktop app. I'll be using it to look at the files which make up a Delta table, but you can also inspect these files through the Fabric UI, it's just a bit harder.

# MARKDOWN ********************

# #### Step 1: Creating a Lakehouse Table
# From that Lakehouse, create a new notebook, which we will use to create some tables and some data. We'll look in much more detail about Lakehouse table creation methods in future tutorials, but for now, run the following Spark SQL script to initialize a table: 

# CELL ********************

# MAGIC %%sql 
# MAGIC CREATE TABLE IF NOT EXISTS PurchaseOrders (
# MAGIC     OrderID STRING,
# MAGIC     CustomerID STRING,
# MAGIC     OrderDate DATE,
# MAGIC     ProductID STRING,
# MAGIC     Quantity INT,
# MAGIC     TotalAmount DECIMAL(10, 2)
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO PurchaseOrders VALUES
# MAGIC ('ORD001', 'CUST001', '2023-10-01', 'PROD001', 3, 150.00),
# MAGIC ('ORD002', 'CUST002', '2023-10-02', 'PROD002', 2, 200.00),
# MAGIC ('ORD003', 'CUST003', '2023-10-03', 'PROD003', 5, 500.00);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Inspect the Lakehouse table and the script above, to understand the structure of the table and the data. For this, you can either: 
# 
# - Use the OneLake File Explorer desktop app, and VS Code, or any code editor, 
# - Or by right-clicking on the Table in the Fabric user interface, and clicking 'View Files'. 
# 
# 
# Structure of your Delta table: 

# MARKDOWN ********************

# ![purchaseorders-delta-table.png](attachment:85957f82-2368-46f3-ba6f-4453d709b03d.png)


# ATTACHMENTS ********************

# ATTA {
# ATTA   "85957f82-2368-46f3-ba6f-4453d709b03d.png": {
# ATTA     "image/png": "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"
# ATTA   }
# ATTA }

# MARKDOWN ********************

# #### Step 2: Inserting data into your Delta table
# 
# Now run the following INSERT statement to add more data into the Delta table, and then inspect how the underlying files have been updated. 

# CELL ********************

# MAGIC %%sql 
# MAGIC INSERT INTO PurchaseOrders VALUES
# MAGIC ('ORD004', 'CUST004', '2023-10-04', 'PROD004', 4, 300.00);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Updating data in your Delta table
# 
# Now run the following UPDATE statement to update existing data in the Delta table, and then inspect how the underlying files have been updated. 

# CELL ********************

# MAGIC %%sql 
# MAGIC UPDATE PurchaseOrders 
# MAGIC SET OrderID = 'ORD010'
# MAGIC WHERE OrderID = 'ORD001'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Deleting data in your Delta table
# 
# Now run the following DELETE statement to delete existing data in the Delta table, and then inspect how the underlying files have been updated. 

# CELL ********************

# MAGIC %%sql 
# MAGIC DELETE FROM PurchaseOrders
# MAGIC WHERE OrderID = 'ORD002';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 5: Return to Skool to complete the reflection for this exercise. 
