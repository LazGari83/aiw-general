# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b12def97-ade4-42ab-9575-345b5d34e5aa",
# META       "default_lakehouse_name": "AD006_RBAC",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "b12def97-ade4-42ab-9575-345b5d34e5aa"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

data = { 
    "customers" : [
        ("C001", "John Doe", "john@example.com"),
        ("C002", "Jane Smith", "jane@example.com")
    ], 
    "employees":  [
        ("EMP01", "Malcolm", 50000),
        ("EMP02", "Shiela", 75000)
    ], 
    "orders": [
        ("O001", "C001", 100),
        ("O002", "C002", 150)
    ]
}

def write_some_data(data: list, name: str)-> None: 
    df = spark.createDataFrame(data)
    df.write.format("delta").mode("overwrite").saveAsTable(name)
    df.write.format("delta").mode("overwrite").save(f"Files/raw/{name}")
    df.write.format("delta").mode("overwrite").save(f"Files/processed/{name}")


for key, value in data.items(): 
     write_some_data(data=value, name=key)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
