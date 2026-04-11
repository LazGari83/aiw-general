# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "72848c38-5bd9-4597-afd8-3e651a6caead",
# META       "default_lakehouse_name": "DQ009_DQS_DataStore",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "72848c38-5bd9-4597-afd8-3e651a6caead"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "1379a247-1c57-9c87-4933-b5f5af0a38e9",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## DQ012 🔶 Validating Semantic Models
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# In this tutorial, we'll focus on the validation of semantic models - then we'll look at deployment of a semantic model to a 'Production' workspace using Semantic Link Labs.   
# 
# #### Prerequisites: 
# - This tutorial assumes you have completed DQ009 and DQ010. 
# - Connect this notebook to your DQ009_DQS_DataStore Lakehouse
# - Connect this notebook to your Environment - this tutorial assumes you have semantic-link-labs 
# - Create a new workspace, which we will deploy the semantic model into, called "Sales Production". 
# 
# #### Step 1: Creating a semantic model 
# Before we can begin to validate a semantic model, first we need to create one! 
# 1. Go to your DQ009_DQS_DataStore Lakehouse, and create a new semantic model called `sales_analytics`, with just one table: `clean_sales`. For now, we won't add any measures or anything else into the semantic model - but these can also be validated using the methods below! 
# 2. Within the settings of the semantic model, we're going to turn auto-updates from OneLake to OFF. 
# 


# MARKDOWN ********************

# #### Step 2: Update your semantic model and read it into the notebook
# Here, we're going to use a combination of Semantic Link (sempy) and its sister package Semantic Link (sempy_labs) Labs. 
# 
# Semantic Link Labs is more experimental that Semantic Link, so bear that in mind for production use cases. 
# 
# In this cell, we: 
# - use `labs.refresh_semantic_model()` to refresh the semantic model (make sure it's up to date - this is assuming you have a semantic model wit auto-refresh off). 
# - use `fabric.read_table()` - this reads a specific table of data from the semantic model into a Fabric Dataframe. These are very much like pandas dataframes, but with additional semantic information (relationships etc). Because of this characteristic of Fabric Dataframes, we can use the Pandas dataframe data source in Great Expectations to validate them. 
# 
# Note: Great Expectations did build and integration to Power BI objects in Fabric, you can check the Microsoft tutorial on the topic [here](https://learn.microsoft.com/en-us/fabric/data-science/tutorial-great-expectations). Please note the code in that tutorial uses an older version of GX (pre 1.0), and that all the techniques shown are also possible by just using the Pandas dataframe data source, which is what we'll be using. 


# CELL ********************

import sempy.fabric as fabric
import sempy_labs as labs

semantic_model_name = "sales_analytics" 
table_name = "clean_sales"

labs.refresh_semantic_model(semantic_model_name)

sm_table = fabric.read_table(semantic_model_name, table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: Run validation against the the semantic model table

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files")

validation_def = context.validation_definitions.get("property_sales_semantic_model_validation")

results = validation_def.run(batch_parameters={"dataframe": sm_table})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Deploy semantic model 
# In this step, I'm showcasing the possibility to deploy a semantic model into another workspace (perhaps for consuming your semantic model/ report), if, and only if, all the validation tests have passed. For this, we use the semantic link labs function: [deploy_semantic_model()](https://semantic-link-labs.readthedocs.io/en/stable/sempy_labs.html#sempy_labs.deploy_semantic_model)
# 
# _Note: this approach has the benefits that it might be simpler to implement for a large majority of Power BI-centric companies. There are other approaches to achieve similar results using Azure DevOps Pipelines, but these would require quite advanced level knowledge of Azure Pipelines/ PowerShell to implement. You will need to plan your CI/CD strategy carefully based on the requirements of your own company._
# 
# **This code assumed you have completed the prerequisite step to create a new Fabric Workspace called "Sales Production", which we will deploy into.**
# 


# CELL ********************

from notebookutils import notebook as nb

if results.success: 
    # deploy the semantic model to the 'production' workspace 
    labs.deploy_semantic_model(
        source_dataset=semantic_model_name, 
        source_workspace="Fabric Dojo 2",
        target_dataset=semantic_model_name, 
        target_workspace="Sales Production", 
        overwrite=True, 
        refresh_target_dataset=False
    )
else: 
    # pass something back to the pipeline and notify
    nb.exit("SM Validation Failed")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 5: Log the validation results
# 
# Finally, we log the validation results. Make sure you update the ABFSS path to your own.  

# CELL ********************

# make sure you update this with your own ABFSS path
dq_validation_logging_table_abfss = "abfss://304b1974-676d-48af-b4e4-8cf6f0fe139e@onelake.dfs.fabric.microsoft.com/72848c38-5bd9-4597-afd8-3e651a6caead/Tables"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd 
from datetime import datetime 

def log_results(results: dict, lakehouse_abfs: str)-> None:
    """Logs the validation results to a DQ Validation Results Lakehouse table (at the given lakehouse_abfs path)

    Inputs: 
        results: dict - the results dictionary returned from a GX validation. 
        lakehouse_abfs: str - the ABFSS path of the DQ Validation Results Lakehouse Table 

    """
    
    # constuct your flattened results
    results_flattened = [
        {
            "validation_id": results.meta['validation_id'], 
            "success": result.success, 
            "test_timestamp": datetime.strptime(results.meta['batch_markers']['ge_load_time'], "%Y%m%dT%H%M%S.%fZ") 
        } for result in results.results] 

    # convert results to a pandas dataframe
    pandas_df = pd.DataFrame(results_flattened)

    # convert to spark dataframe 
    spark_results_df = spark.createDataFrame(pandas_df)
    
    # append this dataframe to the Lakehouse Table 
    spark_results_df.write.mode("append").save(lakehouse_abfs)



#run the logging
log_results(results, dq_validation_logging_table_abfss) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## END
