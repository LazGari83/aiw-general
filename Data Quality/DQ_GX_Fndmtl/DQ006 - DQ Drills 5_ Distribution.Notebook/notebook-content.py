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

# ## DQ006 🟢 DQ Drills 5: Distribution
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the fifth tutorial in the Data Quality (DQ) Drills series. 
# 
# In this series, we are working through a number of practical examples of data quality to: 
# - get accustomed to Great Expectations. We will only be using a small portion of the library in these drill sessions, we will explore more of the GX functionality later in this module! 
# - learn a wide variety of commonly used data quality rulesets, and how they are used, so that you are aware of what's possible and can they apply it to your own datasets. 
# 
# In this exercise, we will explore data distribution. 
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
# Here, we don't need to recreate the pandas data source, but instead only a new CSV asset. 

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files/")

base_directory = "/lakehouse/default/Files/data"

file_name = "dq_distribution_purchases"
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

# ## Validating distribution
# 
# In this section, we'll explore expectations that look at the distribution of data. This includes simple expected ranges for values in a column, but also more commonly for assessing the statistical distribution of the values of a column (this is a prerequisite for many machine learning algorithms). 
# 
# Specifically, we will explore the following Expectations: 
# - Expect column values to be between
# - Expect column value z-scores to be less than
# - Column-level summary statistic Expectations
# 
# 
# #### Expect column values to be between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_values_to_be_between)
# 
# This expectation tests that the values in a column fall between a range of values (the min and the max). What this range is depends on the column - typically it involves some domain knowledge. 
# 
#  

# CELL ********************

expectation = gxe.ExpectColumnValuesToBeBetween(
    column="product_rating",
    min_value=1,
    max_value=5,
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect column value z-scores to be less than
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_value_z_scores_to_be_less_than)
# 
# Checks that the Z-scores (number of standard deviations from mean) of all values are below the given threshold.


# CELL ********************

expectation = gxe.ExpectColumnValueZScoresToBeLessThan(
    column="purchase_amount", threshold=3, double_sided=True
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Column-level summary statistic Expectations
# #### Expect Column Mean To Be Between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_mean_to_be_between)
# 
# 
# 


# CELL ********************

expectation = gxe.ExpectColumnMeanToBeBetween(
    column="purchase_amount", min_value=50, max_value=1000
)
batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Expect Column Quantile Values To Be Between
# [(Documentation for this Expectation)](https://greatexpectations.io/expectations/expect_column_quantile_values_to_be_between)
# 
# This is a really useful Expectation that checks the range values (min, max) of quantiles of data. 
# 
# You provide quantiles, and value_ranges for each quantile, as below.  

# CELL ********************

expectation = gxe.ExpectColumnQuantileValuesToBeBetween(
    column="purchase_amount",
    quantile_ranges={
        "quantiles": [0.5, 0.9],
        "value_ranges": [[50, 200], [500, 2000]],
    },
)

batch.validate(expectation)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## END 
