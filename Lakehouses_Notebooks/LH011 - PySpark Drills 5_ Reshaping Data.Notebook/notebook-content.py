# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "ba2bed90-fdb2-4a5e-9067-23e219d236d3",
# META       "default_lakehouse_name": "LH007_PropertyLH",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "ba2bed90-fdb2-4a5e-9067-23e219d236d3"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # LH011 🔶 PySpark Drills 5: Reshaping Data
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the 5th of five drill-style tutorials. The goal of this mini-series is to expose you to a wide variety of commonly used PySpark functions. 
# 
# In this 5th tutorial in the mini-series, we will look at reshaping data: denormalizing, joining, merging, and more! 
# 
# You'll be given an empty code cell to write the code for each drill. Try to complete each drill without the use of the walkthrough video, but it's there if you need it! 
# 
# #### Prerequisites
# 1. You should already have a Lakehouse in your Fabric Workspace (from the previous exercise) - LH007_PropertyLH 
# 2. Load this notebook into your Fabric Workspace. Connect this notebook to the LH007_PropertyLH Lakehouse. 
# 3. Download the LH011_Datasets.zip file from the Skool tutorial page, unzip the folder, and upload all three files into the Lakehouse Files area: 
# - `property_sales_new_system.csv` 
# - `city_details.csv`
# - `agent_details.csv` 
# 
# 
# #### Data loading from CSV
# 
# **Run the script below to load the `property_sales_new_system.csv` file from `Files/` into a Spark dataframe.** 
# 
# Review the new structure of the property sales dataset. Your client changed the property sales source system, so now you are receiving the data in a different format. 
# 


# CELL ********************

df = (
    spark.read.format("csv")
        .option("header","true")
        .option("inferSchema", True)
        .load("Files/property_sales_new_system.csv")
)

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drill 5.1: Unpivoting data
# So hopefully, after seeing the data, you recognised the need for a bit of unpivoting - or turning a Wide dataset into a narrower dataset. 
# 
# As we have seen in other modules already, having our SalePriceUSD spread across three columns is not ideal. We need to get our SalePriceUSD columns into one column, and for that we can use Spark's UNPIVOT functionality. 
# 
# - PySpark: [df.unpivot()](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.unpivot.html#pyspark.sql.DataFrame.unpivot)
# - Spark SQL: [UNPIVOT()](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-unpivot.html)
# 
# Note: when I was testing both of these in Fabric, only the Spark SQL method worked for me, so I recommend using the Spark SQL. Plus it gives a good opportunity to look at the hand-off between PySpark and Spark SQL again. 
# 
# Using Spark SQL [UNPIVOT()](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-select-unpivot.html), **unpivot the three columns House_SalePriceUSD, Apartment_SalePriceUSD, DetachedHouse_SalePriceUSD into a single column called SalePriceUSD. Assign the resultant dataframe to a variable called sales_unpivoted.** 
# 
# - _Note 1: all other columns should remain the same._ 
# - _Note 2: you will have to create a temporary view from your original `df` to be able to access it using Spark SQL._


# CELL ********************

# create a temporary view from your df
df.createOrReplaceTempView("new_property_sales")
# using Spark SQL UNPIVOT() , convert the three SalePriceUSD columns into one single SalePriceUSD column. Assign the output to sales_unpivoted
sales_unpivoted = spark.sql('''
    SELECT * from new_property_sales
        UNPIVOT(SalePriceUSD FOR type in(House_SalePriceUSD, Apartment_SalePriceUSD, Apartment_SalePriceUSD)
        )
        '''
)

# display the result
display(sales_unpivoted)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drill 5.2: Joining with other datasets
# 
# You have been tasked to prepare a dataset for the data science team. They are looking to predict the SalePriceUSD for a property, given all the other information that might be known about a property. 
# 
# They have asked you to look into the source system and extract and then merge any new, relevant information into the `sales_unpivoted` dataset that you think might help to predict SalePrice USD. 
# 


# CELL ********************

# load the city_details dataset into a cities_df dataframe 
cities_df = (
    spark.read.format("csv")
        .option("header","true")
        .option("inferSchema", True)
        .load("Files/city_details.csv")
)
# display the dataframe 
display(cities_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# load the agent details dataset into agents_df dataframe
agents_df = (
    spark.read.format("csv")
        .option("header","true")
        .option("inferSchema", True)
        .load("Files/agent_details.csv")
)

# display the agents_df dataframe 
display(agents_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# create a full_sales_dataset dataframe, which joined the sales_unpivoted dataframe to the two other dataframes 
full_sales_dataset = (
    sales_unpivoted
        .join(agents_df,sales_unpivoted.Agent == agents_df.AgentName,"left")
        .join(cities_df,"City","Left")
)
# display 
display(full_sales_dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Drill 5.3: Final dataset cleanup 
# 
# After showing the dataset to you data scientist, they have requested a few small changes to the structure. They have asked you to: 
# - remove the AgentID & AgentName columns 
# - add a prefix of 'City' to the column that came from the city_details dataset - to make it clear what this data represents. 
# 
# Using whichever method you prefer, **make the required changes to your dataframe, and finally load the final dataframe into your Lakehouse, ready for consumption by the data science team:**

# CELL ********************

final_df = full_sales_dataset.drop('AgentID', 'AgentName')

renames = {'AvgSalePrice': 'CityAvgSalePrice', 'MedianIncome': 'CityMedianIncome', 'Population': 'CityPopulation'}

final_df = final_df.withColumnsRenamed(renames)

final_df.write.format('delta').mode('overwrite').saveAsTable('sales_ml_prep_data')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### END
