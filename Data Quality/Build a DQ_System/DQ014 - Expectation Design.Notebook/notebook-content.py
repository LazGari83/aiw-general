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

#  import GX

# CELL ********************

import great_expectations as gx
import great_expectations.expectations as gxe

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  intialize our contxt

# CELL ********************

context = gx.get_context(mode="file", project_root_dir="/lakehouse/default/Files")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

#  look at the Expectation Suites in the Context

# CELL ********************

context.suites.all()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# get a specific expectation suite from the context

# CELL ********************

suite = context.suites.get("sales_incoming_expectations")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# define an expectation from gxe package

# CELL ********************

expectation =[
gxe.ExpectTableColumnsToMatchSet(column_set=["SaleID", "Address", "Type", "City", "SalePriceUSD", "Agent", "Transaction_TS"])
, gxe.ExpectColumnValuesToNotBeNull(column="Type")
, gxe.ExpectColumnDistinctValuesToBeInSet(
column="Type",
value_set=[
"Apartment"
, "Detached House"
, "House"
]
)
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# add the new expectation to the suite

# CELL ********************

# The variable should be named 'expectation' instead of 'expectations'.
# This fixes the NameError by using the correct variable defined earlier.
for ex in expectation:
    suite.add_expectation(ex)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(ex.expectation_type, 'has been added successfully')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
