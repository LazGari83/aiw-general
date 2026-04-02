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

# ## DQ002  🟢 DQ Drills 1: Schema Validation 
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# 
# Welcome to the first in the Data Quality (DQ) Drills series. 
# 
# In this series, we will work through a number of practical examples of data quality; in the process you will: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# #### Prerequisites
# - You should have a Lakehouse in your Fabric Workspace (called DQ002_DQDrills), and you should connect that Lakehouse to your Notebook.   
# 
# #### Set up
# We will install the Great Expectations library 'inline' just for convenience. In practice, you might want to create an [Environment](https://learn.microsoft.com/en-us/fabric/data-engineering/create-and-use-environment), and install great-expectations into the Environment. Here, we are declaring the version number explicitly (1.2.4), to protect against future updates to the library intoducing errors (like what happened previously!). 
# 
# The --q means "quiet mode" - it prevents it from printing out a load of dependencies that are installed in the installation process. 


# CELL ********************

# install GX 
%pip install great-expectations==1.2.4 --q


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Import the required packages: 

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Initalize a 'Context' at the specified directory. Here we are just using the Files/ directory of a Lakehouse. In future, we will look at where to store your GX context in Enterprise scenarios. 

# CELL ********************

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files/")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# **What is a Context?**
# 
# The 'Context' is the central concept in any GX implementation. You can think of the context as the configuration. 
# 
# Specifically, when setting up GX, we configure: 
# - the data sources we want to validate. This could be a Spark datasource (for Lakehosue Tables, Warehouse Tables or Spark dataframes), or it could be a file datasource, or many more datasources are supported. 
# - the 'Expectations' - an expectation is a data quality ruleset that you want to validate fo the dataset. For example, *"for this Lakehouse Table, I expect all values in the ID column to be unique"*
# 
# When we are setting up GX, we walk through a number of configuration steps, but then crucially, **your Context is stored in the Lakehouse Files directory you specify, this means that in 'runtime' aka when you want to actually validate new incoming data, or new Spark dataframe data, we simply pickup the Context you specified previously, and pass in the new data to be validated.**  
# 
# #### Registering our dataset
# The following code block performs quite a few steps in one go: 
# - **add_pandas_filesystem** : we add a new Pandas data source to `context.data_sources` (note: GX uses Pandas for file validation, so any file type that you can load with pandas read_* can be validated using GX)
# - **add_csv_asset**: we add a CSV data asset to the Context , 
# - **add_batch_definition_path** - creates a new Batch Definition
# - **batch_definition.get_batch** - gets a Batch from the Batch Definition
# 
# This is a little painful, but you only need to declare it once and then it's all set up (forever 😀). 
# 
# The Batch is what we can then use for validation. 


# CELL ********************

base_directory = "/lakehouse/default/Files/data"
file_name = "dq_schemas"
batch_definition = (
    context.data_sources
        .add_pandas_filesystem(name="DQ001_Files", base_directory=base_directory)
        .add_csv_asset(name=file_name)
).add_batch_definition_path(name=f"{file_name}_batch_definition", path=f"{file_name}.csv")

batch = batch_definition.get_batch()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Schema Validation of a dataset (what's possible?)
# 
# Great Expectations has a number of pre-built data quality rulesets (see them all [here](https://greatexpectations.io/expectations/)). 
# 
# In the following section, we will explore some of the Expectations related to Schema Validation: 
# - Expect Column Values To Be Of Type
# - Expect Column To Exist
# - Expect Table Column Count To Equal
# - Expect Table Columns To Match Ordered List
# - Expect Table Columns To Match Set
# 
# 
# 
# #### Expect Column Values To Be Of Type
# [Documentation for this Expectation](https://greatexpectations.io/expectations/expect_column_values_to_be_of_type)
# 
# This Expectation checks that every value in the given column can be parsed as the Type given - very useful when reading CSV data (which doesn't have a schema embedded in it). 

# CELL ********************

expectation = gxe.ExpectColumnValuesToBeOfType(column="transfer_amount", type_="float64")
batch.validate(expectation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Column To Exist
# [Documentation for this Expectation](https://greatexpectations.io/expectations/expect_column_to_exist)
# 
# This expectation checks that an important column exists in the dataset, before proceeding with downstream processing. 

# CELL ********************

expectation = gxe.ExpectColumnToExist(column="sender_account_number")
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Table Column Count To Equal
# [Documentation for this Expectation](https://greatexpectations.io/expectations/expect_table_column_count_to_equal)
# 
# This expectation checks that the dataset has a fixed number of columns (it doesn't check what these columns are though):
# 


# CELL ********************

expectation = gxe.ExpectTableColumnCountToEqual(value=5)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Table Columns To Match Ordered List
# [Documentation for this Expectation](https://greatexpectations.io/expectations/expect_table_columns_to_match_ordered_list)
# 
# This expectation checks that the columns in the dataset match exactly (including the order) what is supplied in the `column_list` parameter. 


# CELL ********************

expectation = gxe.ExpectTableColumnsToMatchOrderedList(
    column_list=[
        "type",
        "sender_account_number",
        "recipient_fullname",
        "transfer_amount",
        "transfer_date"
    ]
)

batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Table Columns To Match Set
# [Documentation for this Expectation](https://greatexpectations.io/expectations/expect_table_columns_to_match_set)
# 
# This expectation checks for the existence of a set of columns, but they can be in any order. 
# 


# CELL ********************

expectation = gxe.ExpectTableColumnsToMatchSet(
    column_set=[
        "sender_account_number",
        "recipient_fullname",
        "type",
        "transfer_amount",
        "transfer_date"
    ],
    exact_match=True,
)

batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
