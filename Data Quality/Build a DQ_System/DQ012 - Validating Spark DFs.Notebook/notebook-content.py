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

# ## DQ012 🔶 Validating Spark DFs
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# In this tutorial, we'll focus on how to validate Spark Dataframes, and then how to handle the results. 
# 
# #### Prerequisites: 
# - This tutorial assumes you have completed DQ009 and DQ010. 
# - Connect this notebook to your DQ009_DQS_DataStore Lakehouse
# - Connect this notebook to your Environment.  
# 


# MARKDOWN ********************

# #### Step 1: Clean raw_sales
# 
# In this notebook, we are going to read our `raw_sales` table into a Spark Dataframe, perform some cleaning on that Dataframe, validate the Dataframe, before loading it into a new table. 
# 
# We will perform 'row quarantining' in this example - creating a new column in our dataset, and updating each row if it passed or failed the validation.  

# CELL ********************

from pyspark.sql.functions import regexp_replace, to_timestamp

# get raw data 
df = spark.read.table("raw_sales")

# run some transformation on the data (I've just copied the code we went through in LH0)
clean_df = (
    df
        .drop_duplicates(['SaleID'])
        .withColumn("SalePriceUSD", regexp_replace("SalePriceUSD", "[^0-9]", "").cast("int"))
        .withColumn("Transaction_TS", to_timestamp("Transaction_TS", "dd/MM/yyyy HH:mm"))
        .withColumn("SaleID", df.SaleID.cast("int"))      
        .dropna(subset='SalePriceUSD')
        .filter((df.SalePriceUSD > 100000) & (df.SalePriceUSD < 30000000))
)

display(clean_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2: Run validation against the clean_df
# 
# To perform the 'row quarantining' - we need to be a bit more specific about the `results_format`. In Great Expectations, we can tell it how we want it to format the results object. The result_format:"COMPLETE" is what we need for get the SaleIDs of the rows that fail validation. 


# CELL ********************

# import GX 
import great_expectations as gx
import great_expectations.expectations as gxe

# intialize our contxt
context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files")

# # get the validation definition from the Context
validation_def = context.validation_definitions.get("clean_property_sales_df_validation")

# # run the validation, returning the results 
results_format = {"result_format": "COMPLETE",  "unexpected_index_column_names": ["SaleID"]}

results = validation_def.run(batch_parameters={"dataframe": clean_df}, result_format=results_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3: "Quarantine" invalid data 
# 
# 
# 
# In this cell, the goal is to extract the SaleID's from the rows that failed one or more of the validation tests - we get this from the GX validation results object. 
# 
# Then we write this information into 

# CELL ********************

from pyspark.sql.functions import when, current_timestamp
from pyspark.sql import DataFrame


def get_SaleIDs_of_invalid_rows(results_object: dict) -> list:
    """ Parses the results object to extract SaleIDs of rows that have failed validation. 
    Input: 
        - results_object: dict - a GX results object. 


    """
    unexpected_sale_ids = [
        entry["SaleID"]
        for result in results_object.get("results", [])
        for entry in result.get("result", {}).get("unexpected_index_list", [])
        if "SaleID" in entry
    ]

    return unexpected_sale_ids

def add_validation_results_to_df(df: DataFrame, column_identifier: str, failure_ids:list ) -> DataFrame:

    return df.withColumn("passed_validation", when(df[column_identifier].isin(failure_ids), False).otherwise(True))

# Call the function and print the result
invalid_ids = get_SaleIDs_of_invalid_rows(results)

# prepare the final dataframe
final_df = add_validation_results_to_df(clean_df, 'SaleID', invalid_ids)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 4: Merge into Clean table

# CELL ********************

import os
from delta.tables import DeltaTable

table_exists_check= os.path.exists('/lakehouse/default/Tables/clean_sales')

if table_exists_check == False: 
    final_df.write.format("delta").mode("overwrite").saveAsTable("clean_sales")

else: 

    # get our hc delta table 
    clean_sales_table = DeltaTable.forName(spark, "clean_sales")

    ( 
        clean_sales_table
            .alias("target")
            .merge(final_df.alias("source"),"target.SaleID = source.SaleID")
            .whenMatchedUpdateAll() 
            .whenNotMatchedInsertAll() 
            .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 5: Log the validation results

# CELL ********************

# update this cell with your ABFSS path
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



#run the 
log_results(results, dq_validation_logging_table_abfss) 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## END
