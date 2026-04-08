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

# ## DQ008 🟢 DQ Drills 7: Data Volume
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the seventh tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore data volume. 
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
file_name = "dq_volume_financial_transfers"

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

# ## Validating Data Volume
# 
# Have you ever had it when the dataset that you were analysing normally gives you say 100 records each month, but for some reason, one month, it only gives you 10 records? It can be very difficult to notice this, until someone in the business points it out to you. 
# 
# Well, no more, finally you can build protections against this type of data quality issue. 
# 
# When you have a dataset that should give you a predefined number of rows each time (or within a given range) - then you can use a data volume ruleset in GX. 
# 
# So in this notebook, we will explore the following options:  
# - Expect Table Row Count To Be Between
# - Expect Table Row Count To Equal
# 
# #### Expect Table Row Count To Be Between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between)
# 
# This Expectation checks that the row count is between the min and the max. Now sometimes this can be hard-coded, if the row count never changes over time, or you can create dynamic threshold for data volume. 
# 
# For this second option, normally this would involve storing data volumes of each load, and using these historic values to create a dynamic threshold banding, that is then passed into the Expectation at runtime (this is outside the scope of this tutorial though!)
# 
# 


# CELL ********************

expectation = gxe.ExpectTableRowCountToBeBetween(min_value=30, max_value=40)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Table Row Count To Equal
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_table_row_count_to_equal)
# 
# This expectation checks the row count is exactly equal to the parameter value. 
# 
# Say for example, you were ingesting monthly sales figures for the year of 2023. Here you 'expect' the row count to be 12. 
# 
# Obviously, you need to be careful with this one, this test can be particularly restrictive (but for some datasets, this rigour is necessary)

# CELL ********************

expectation = gxe.ExpectTableRowCountToEqual(value=38)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
