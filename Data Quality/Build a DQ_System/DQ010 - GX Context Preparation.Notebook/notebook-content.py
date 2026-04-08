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

# ## DQ010 🔶 GX Context Preparation
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# In this tutorial, we will configure our Great Expectations Context for our project! This is an essential first step in the process. 
# 
# In the notebook below, we will , we will go through the following steps: 
# 1. Register the data source (this could be a flat-file, a Spark DF (can be used to test Lakehouse tables or Warehouse tables also), or a Semantic Model. 
# 2. Define a list of GX Expectations for that dataset
# 3. Create a validation definition which ties together the dataset and the expectations. 
# 
# And we will go through this process for the three different datasets (or rather, the same dataset, but at different stages in the pipeline): 
# 1. The incoming raw file 
# 2. A Spark Dataframe (that we will validate after we clean/transform our data, before writing it to a Lakehouse table)
# 
# At the end of the notebook, we perform a refactoring exercises to make it as quick and easy as possible to register new datasets into your GX Context. 


# MARKDOWN ********************

# #### Prerequisites
# - Connect this notebook to your DQ009_DQS_DataStore you created in  DQ009. 
# - Connect this notebook to the Environment you created in DQ009. 


# CELL ********************

# import GX 
import great_expectations as gx
import great_expectations.expectations as gxe

# intialize our contxt
context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data source 1: Incoming CSV File 
# #### Step 1.1: Connect to the data source
# We start by configuring our Incoming File data source - this is of type "pandas filesystem". GX uses pandas to load and validate files. 
# 
# Notice how this time, we are only defining the Batch Definition - we are not initializing the Batch, like we did in the fundamentals exercises. This is because for the moment, we're not actually doing any validation, simply defining what we want to validate in the future. 

# CELL ********************

base_directory = "/lakehouse/default/Files/landing_zone"
file_name = "property-sales-messy"

batch_definition = (
    context.data_sources
        .add_pandas_filesystem(name="DQS", base_directory=base_directory)
        .add_csv_asset(name=file_name)
).add_batch_definition_path(name=f"{file_name}_batch_definition", path=f"{file_name}.csv")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1.2: Configure a single expectation
# For the moment, we will only declare a single Expectation of our dataset. I just want to setup the entire end-to-end Context. 
# 
# In the future (in DQ014), we will explore in-depth which Expectations make sense for each dataset - and we can update our Context at that later point. For now the focus is on setting up an 'MVP' system. 

# CELL ********************

# create a list, and define individual Expectations within that list
expectation = gxe.ExpectTableColumnsToMatchSet(column_set=["SaleID", "Address", "Type", "City", "SalePriceUSD", "Agent", "Transaction_TS"])

# create an Expectation suite
suite = gx.ExpectationSuite(name="sales_incoming_expectations")

# add the (empty) Expectation Suite to your context 
suite = context.suites.add(suite)

# add the expectation to the suite
suite.add_expectation(expectation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 1.3: Create the validation definition
# 
# The Validation Definition essentially binds together the Batch Definition and the Expectation Suite. 
# 
# The Validation Definition is important, because it is what is used in the 'runtime' scenario. We will look at this further below, but first, let's just define our Validation Definition - don't worry this one's quite simple! 

# CELL ********************

# create a Validation Definition
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="sales_incoming_file_validation"
)

# add the Validation Definition to our context 
context.validation_definitions.add(validation_definition)

# check that it has been successfully added: 
context.validation_definitions.get("sales_incoming_file_validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Source 2: Clean Spark DF 
# Next, let's focus on setting up a Spark DF data source in our Context. We go through exactly te same 3-step process, the only real difference is the data source type. 
# 
# #### Step 2.1: Connect to the data source

# CELL ********************

lakehouse_name = "DQS_DataStore"
table_name = "clean_property_sales"

batch_definition  = (
    context.data_sources
        .add_spark(name=lakehouse_name) # this is my data source name (the Lakehouse) 
        .add_dataframe_asset(name=table_name) # this is my Lakehouse table name
    ).add_batch_definition_whole_dataframe(f"{table_name}_batch_definition") # defining a batch of the whole dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2.2: Configure a single expectation


# CELL ********************

# create a list, and define individual Expectations within that list
expectation = gxe.ExpectColumnValuesToBeBetween(column="SalePriceUSD", min_value=100000, max_value=30000000)

# create an Expectation suite
suite = gx.ExpectationSuite(name="clean_property_sales_df_expectations")

# add the (empty) Expectation Suite to your context 
suite = context.suites.add(suite)

# add the expectation to the suite
suite.add_expectation(expectation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 2.3: Create the validation definition

# CELL ********************

# create a Validation Definition
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="clean_property_sales_df_validation"
)

# add the Validation Definition to our context 
context.validation_definitions.add(validation_definition)

# check that it has been successfully added: 
context.validation_definitions.get("clean_property_sales_df_validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Source 3: Semantic Model
# Finally, we declare our semantic model data source - for this, we use `add_pandas()`. Why pandas? We will be using Semantic Link to access the data inside our semantic models, and Semantic Link provides us with an object of type Fabric Dataframe - this is based on the Pandas Dataframe. Therefore we can pass in the Fabric Dataframe into GX as a Pandas data source. 
# 
# Don't worry we'll explain this process in more detail in DQ013, for now, we just need to register the data source. 
# 
# #### Step 3.1: Connect to datasource 

# CELL ********************

semantic_model_name = "PropertySales_Semantic_Model"
table_name = "semantic_model_fact_sales"

batch_definition  = (
    context.data_sources
        .add_pandas(name=semantic_model_name) # this is my data source name (the Lakehouse) 
        .add_dataframe_asset(name=table_name) # this is my Lakehouse table name
    ).add_batch_definition_whole_dataframe(f"{table_name}_batch_definition") # defining a batch of the whole dataframe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3.2: Configure a single expectation


# CELL ********************

# create a list, and define individual Expectations within that list
expectation = gxe.ExpectColumnValuesToBeUnique(column="SaleID")

# create an Expectation suite
suite = gx.ExpectationSuite(name="property_sales_semantic_model_expectations")

# add the (empty) Expectation Suite to your context 
suite = context.suites.add(suite)

# add the expectation to the suite
suite.add_expectation(expectation)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Step 3.3: Create a validation definition

# CELL ********************

# create a Validation Definition
validation_definition = gx.ValidationDefinition(
    data=batch_definition, suite=suite, name="property_sales_semantic_model_validation"
)

# add the Validation Definition to our context 
context.validation_definitions.add(validation_definition)

# check that it has been successfully added: 
context.validation_definitions.get("property_sales_semantic_model_validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Tutorial Bonus (⬛): Refactoring the codebase to make it easier/ quicker to register any dataset
# You might have noticed that there is some repition in the code needed to run through the three steps for registering new datasets. 
# 
# Let's refactor the code to make it quick and easy to register new datasets (from any source): 

# CELL ********************

# function to register add any new data source to the context
# import GX 
import great_expectations as gx
import great_expectations.expectations as gxe

# intialize our contxt
context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files")

def create_batch_definition(type, params): 
    """ func to register any datasource in a GX Context
    Inputs: 
        type - string - one of "csv", "spark_df" or "semantic_model"
        params - object - {"datasource_name": "", "dataasset_name":"", "base_directory":"" }
    Output: GX Batch Definition
    """

    batch_definition = None
    if type == 'csv': 
        batch_definition = (
        context.data_sources
            .add_pandas_filesystem(name=params['datasource_name'], base_directory=params['base_directory'])
            .add_csv_asset(name=params['dataasset_name'])
        ).add_batch_definition_path(name=f"{params['dataasset_name']}_batch_definition", path=f"{params['dataasset_name']}.csv")
    
    elif type == 'spark_df': 
        batch_definition  = (
            context.data_sources
                .add_spark(name=params['datasource_name']) 
                .add_dataframe_asset(name=params['dataasset_name'])
            ).add_batch_definition_whole_dataframe(f"{params['dataasset_name']}_batch_definition")

    elif type == 'semantic_model': 
        batch_definition  = (
            context.data_sources
                .add_pandas(name=params['datasource_name']) 
                .add_dataframe_asset(name=params['dataasset_name']) 
            ).add_batch_definition_whole_dataframe(f"{params['dataasset_name']}_batch_definition")
    
    return batch_definition

def register_any_datasource(type=None, params = {}, expectations_list=[]) -> None: 

    """ This functions registers any datasource in your GX Context 
    Inputs: 
        type - string - one of "csv", "spark_df" or "semantic_model"
        params - object - {"datasource_name": "", "dataasset_name":"", "base_directory":"" }
        expectations_list - list - a list of GX Expectations for that dataset.  
    """

    batch_definition = create_batch_definition(type, params)

    # create an Expectation suite
    suite = gx.ExpectationSuite(name=f"{params['dataasset_name']}_expectations")
    suite = context.suites.add(suite)
    [suite.add_expectation(expectation) for expectation in expectations_list]

    # create a Validation Definition
    validation_definition = gx.ValidationDefinition(data=batch_definition, suite=suite, name=f"{params['dataasset_name']}_validation")
    context.validation_definitions.add(validation_definition)
    context.validation_definitions.get(f"{params['dataasset_name']}_validation")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Run your function
# _Note: if you run this after all the previous cells, it won't work, because you've already registered this data source in the GX Context._ 
# 
# _What you can do if you want to test it is: run context.data_sources.delete("DQS") to delete our first data source, and then run the code below to generate it again._

# CELL ********************

# run this if you want to test the register_any_datasource() function 
context.data_sources.delete("DQS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

csv_params = {"datasource_name": "DQS", "dataasset_name":"property-sales-messy", "base_directory":"/lakehouse/default/Files/landing_zone" }

expectations_list = [gxe.ExpectTableColumnsToMatchSet(column_set=["SaleID", "Address", "Type", "City", "SalePriceUSD", "Agent", "Transaction_TS"])]

register_any_datasource(type='csv', params=csv_params, expectations_list=expectations_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## END 
