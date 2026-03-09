# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ba2bed90-fdb2-4a5e-9067-23e219d236d3",
# META       "default_lakehouse_name": "LH007_PropertyLH",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "ba2bed90-fdb2-4a5e-9067-23e219d236d3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


from notebookutils import notebook as nb

nb.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ws_notebooks = nb.list()
ws_notebooks

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook_ids = [x.displayName for x in ws_notebooks if "Drills" in x. displayName]
notebook_ids

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb.run(notebook_ids[3],90,{'useRootDefaultLakehouse': True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb.help("runMultiple")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb.runMultiple([notebook_ids[1],notebook_ids[2]])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DAG = {
    "activities": [
        {
            "name": notebook_ids[0], # activity name, must be unique
            "path": notebook_ids[0], # notebook path
            "timeoutPerCellInSeconds": 90, 
          
        },
        {
            "name": notebook_ids[1],
            "path": notebook_ids[1],
            "timeoutPerCellInSeconds": 120,
            "retry": 1,
            "retryIntervalInSeconds": 10,
            "dependencies": [notebook_ids[0]] # list of activity names that this activity depends on
        }
    ],
    "timeoutInSeconds": 43200, # max timeout for the entire DAG, default to 12 hours
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb.validateDAG(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

nb.runMultiple(DAG,{"dispalyDAGViaGraphicciz": True})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
