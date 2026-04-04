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

# ## DQ003  🔶 DQ Drills 2: Missing Data
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the second tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore missing data/ NULL values. 
# 
# #### Prerequisites
# - You should have a Lakehouse in your Fabric Workspace (called DQ002_DQDrills), and you should connect that Lakehouse to this Notebook.   
# 
# #### Set up
# Pieces of the code-base that were explained in DQ Drills 1, will not be explained again, for brevity. Of course, all the new bits of code will be explained! 


# CELL ********************

# install GX 
%pip install great-expectations==1.2.4 --q


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Read the existing 'Context' from the specified directory. 
# Here, we don't need to recreate the pandas data source, but instead only new CSV assets. 

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files/")

base_directory = "/lakehouse/default/Files/data"
file_name = "dq_missingness"

batch_definition = (
    context.data_sources
        .get("DQ001_Files") # get the EXISTING data source (that we set up in DQ001)
        .add_csv_asset(name=file_name)
).add_batch_definition_path(name=f"{file_name}_batch_definition", path=f"{file_name}.csv")

batch = batch_definition.get_batch()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Testing for missing data 
# 
# This is a classic - testing columns for NULL values. Some columns, you expect require NO NULL VALUES, and in some columns, you might expect every value to be NULL (like in the first example we are about to see)
# 
# So in this notebook, we will explore the following options:  
# - Expect Column Values To Be Null
# - Expect Column Values Not To Be Null
# 
# #### Expect Column Values To Be Null
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_values_to_be_null)
# 
# This expectation tests that all the values in the column are NULL. This is useful for columns like 'Errors' that you sometimes get in source systems. They should be NULL, unless there is a problem - which would be useful to know about. 


# CELL ********************

expectation = gxe.ExpectColumnValuesToBeNull(column="errors")
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Column Values To Not Be Null
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)
# 
# This expectation is the opposite of the previous one... it tests that all the values in the column are NOT NULL. This is particularly useful for important columns like Keys you will likely join on in the future. Also useful for machine learning label columns or important date columns that will be used for time intelligence calculations. 
# 


# CELL ********************

expectation = gxe.ExpectColumnValuesToNotBeNull(column="transfer_amount")
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# You can also pass in the `mostly` input parameter. In the example below, I provide a `mostly` value of 0.75, which means that the test will pass if 75% (or more) of the column values are NOT NULL. Anything less than 75% and the test will fail. 

# CELL ********************

expectation = gxe.ExpectColumnValuesToNotBeNull(column="transfer_date", mostly=0.75)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
