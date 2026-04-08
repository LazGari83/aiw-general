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

# ## DQ005 🟢 DQ Drills 4: Data Integrity
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the fourth tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore data integrity. 
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
# 
# In this exercise, we create two new csv assets - one for the transfer balance, and one for the transfer transactions.

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files/")

base_directory = "/lakehouse/default/Files/data"

# first dataset 
file_name_1 = "dq_integrity_transfer_balance"
batch_definition = (
    context.data_sources
        .get("DQ001_Files") # get the EXISTING data source (that we set up in DQ001)
        .add_csv_asset(name=file_name_1)
).add_batch_definition_path(name=f"{file_name_1}_batch_definition", path=f"{file_name_1}.csv")

batch_transfer_balance = batch_definition.get_batch()

# second dataset
file_name_2 = "dq_integrity_transfer_transaction"

batch_definition = (
    context.data_sources
        .get("DQ001_Files") # get the EXISTING data source (that we set up in DQ001)
        .add_csv_asset(name=file_name_2)
).add_batch_definition_path(name=f"{file_name_2}_batch_definition", path=f"{file_name_2}.csv")

batch_transfer_transaction = batch_definition.get_batch()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Validating integrity
# 
# In this section, we'll explore rulesets that look at the integrity of data. 
# 
# Specifically, we will explore the following Expectations: 
# - Expect column pair values to be equal
# - Expect multicolumn sum to equal
# - Expect column pair values A to be greater than B
# 
# 
# #### Expect column pair values to be equal
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_pair_values_to_be_equal)
# 
# This expectation tests that the value is column A is equal to the value in column B.  

# CELL ********************

expectation = gxe.ExpectColumnPairValuesToBeEqual(
    column_A="sender_ref_no", column_B="recipient_conf_code"
)
batch_transfer_transaction.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect multicolumn sum to equal
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_multicolumn_sum_to_equal)
# 
# This expectation tests that the sum of row values is equal to the specified sum_total. 
# 
# In this example, we checking that the sum of the values across the `adjustment`, `sender_debit` and `recipient_credit` columns is 0. 
# 


# CELL ********************

expectation = gxe.ExpectMulticolumnSumToEqual(
    column_list=["adjustment", "sender_debit", "recipient_credit"], sum_total=0
)
batch_transfer_balance.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect column pair values A to be greater than B
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_pair_values_a_to_be_greater_than_b)
# 
# This expectation tests that the values in the provided column A are greater (or equal if or_equal is set to True) than the values of column B. 
# 
# In the example shown, we are testing that the received timestamp is greater than the sent timestamp. 


# CELL ********************

expectation = gxe.ExpectColumnPairValuesAToBeGreaterThanB(
    column_A="received_ts", column_B="sent_ts", or_equal=True
)
batch_transfer_transaction.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
