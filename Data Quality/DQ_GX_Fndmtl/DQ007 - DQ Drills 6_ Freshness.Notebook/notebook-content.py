# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "59ff9e28-7238-4401-a0d9-52dd02a9c1ed",
# META       "default_lakehouse_name": "DQ002_DQDrills",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "59ff9e28-7238-4401-a0d9-52dd02a9c1ed"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## DQ007 🟢 DQ Drills 6: Freshness
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the sixth tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore data freshness. 
# 
# #### Prerequisites
# - You should have a Lakehouse in your Fabric Workspace (called DQ002_DQDrills), and you should connect that Lakehouse to this Notebook.   
# 
# #### Set up


# CELL ********************

# install GX 
%pip install great-expectations==1.2.4 --q


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files/")

base_directory = "/lakehouse/default/Files/data"

file_name = "dq_freshness_sensor_readings"
batch_definition = (
    context.data_sources
        .get("DQ001_Files") 
        .add_csv_asset(name=file_name)
).add_batch_definition_path(name=f"{file_name}_batch_definition", path=f"{file_name}.csv")

batch = batch_definition.get_batch()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Validating data freshness 
# 
# In this section, we'll explore rulesets that look at the freshness of data.  
# 
# Specifically, we will explore the following Expectations: 
# - Expect column maximum to be between
# - Expect column minimum to be between
# 
# 
# #### Expect column maximum to be between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_max_to_be_between)
# 
# We can use the ExpectColumnMaximumToBeBetween expectation to test that the maximum value in a timestamp column is within an expected range (i.e. in the last 24 hours). 
#  

# CELL ********************

expectation = gxe.ExpectColumnMaxToBeBetween(
    column="reading_ts",
    min_value="2024-11-22 14:42:00",
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect column minimum to be between 
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
# 
# We can use the ExpectColumnMinimumToBeBetween expectation to test for the opposite - that the first timestamp in a dataset falls within a given range. For example, if you know that you dataset _should_ start at 2024-01-01 00:00:00 - now you can test for that. 


# CELL ********************

expectation = gxe.ExpectColumnMinToBeBetween(
    column="reading_ts",
    min_value="2024-11-22 00:00:00",
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
