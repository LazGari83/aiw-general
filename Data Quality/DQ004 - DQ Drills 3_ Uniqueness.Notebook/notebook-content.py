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

# ## DQ004 🟢 DQ Drills 3: Uniqueness
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the third tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore duplicate values and uniqueness. 
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
file_name = "dq_uniqueness_customers"

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

# ## Validating uniqueness
# 
# In this section, we'll explore rulesets that identify duplicate values in a variety of different ways. 
# 
# Duplicate values on key columns can lead to problems when we join the dataset with other datasets. 
# 
# For machine learning use cases, duplicate rows can erroneously impact the distribution of a dataset. 
# 
# So in this notebook, we will explore the following options:  
# - Expect column values to be unique
# - Expect compound columns to be unique
# - Expect column proportion of unique values to be between
# - Expect column unique value count to be between
# - Expect select column values to be unique within record
# 
# 
# #### Expect column values to be unique
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_values_to_be_unique)
# 
# This expectation tests that each value in a column is unique (no duplicates!)

# CELL ********************

expectation = gxe.ExpectColumnValuesToBeUnique(column="customer_id")
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect compound columns to be unique
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_compound_columns_to_be_unique)
# 
# This expectation tests for uniqueness of values across more than one column. 
# 
# This can be useful for datasets that will be joined using two (or more) columns. 
# 


# CELL ********************

expectation = gxe.ExpectCompoundColumnsToBeUnique(
    column_list=["country_code", "government_id"],
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect column proportion of unique values to be between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_proportion_of_unique_values_to_be_between)
# 
# The 'proportion' in this context means the number of unique values, as a proportion of total number of all values in that column. 
# 
# For example, in a column containing [1, 2, 2, 3, 3, 3, 4, 4, 4, 4], there are 4 unique values and 10 total values for a proportion of 0.4. 


# CELL ********************

expectation = gxe.ExpectColumnProportionOfUniqueValuesToBeBetween(
    column="email_address", min_value=0.9, max_value=1.0
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect column unique value count to be between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_unique_value_count_to_be_between)
# 
# This expectation tests that the count of unique values in a column is between the min and max values provided. 

# CELL ********************

expectation = gxe.ExpectColumnUniqueValueCountToBeBetween(
    column="country_code", min_value=1, max_value=5
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect select column values to be unique within record
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_select_column_values_to_be_unique_within_record)
# 
# This expectation is slightly unique, in that it tests for row-wise duplicate data. In other words, you supply a column list, and for each record, it will check that a value has not been duplicated across each column (in the column list). 
# 
# In the example below, we check to see if the email_address and secondary_email are duplicates. 
# 
# Another example of when this might be helpful would be for survey data; checking for yes's across multiple columns (which might be an error). 
# 


# CELL ********************

expectation = gxe.ExpectSelectColumnValuesToBeUniqueWithinRecord(
    column_list=["email_address", "secondary_email"],
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
