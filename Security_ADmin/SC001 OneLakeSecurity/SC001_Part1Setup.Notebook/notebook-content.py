# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# ## SC001 🔶 Implement OneLake Security - SETUP SCRIPTS
# > ⚠️ **DO NOT "RUN ALL" THIS NOTEBOOK**. Run each step individually, because Step 4 is manual.  
# >
# 
# #### Step 1: install the Fabric CLI

# CELL ********************

!pip install ms-fabric-cli --q

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 2: prepare the tokens (using your user identity)

# CELL ********************

token = notebookutils.credentials.getToken('pbi')
os.environ['FAB_TOKEN'] = token
os.environ['FAB_TOKEN_ONELAKE'] = token

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 3: create the infrastructure
# > **note: you must change the Capacity Name variable to the name of your Capacity, otherwise it won't work**

# CELL ********************

# MAGIC %%sh
# MAGIC 
# MAGIC CAPACITY_NAME="learningcapacity2"
# MAGIC 
# MAGIC fab config set default_capacity $CAPACITY_NAME
# MAGIC 
# MAGIC fab mkdir OLS_Gold_.Workspace
# MAGIC fab mkdir OLS_Marketing_.Workspace
# MAGIC fab mkdir OLS_Sales_.Workspace
# MAGIC 
# MAGIC fab create OLS_Gold_.Workspace/OLS_Gold_LH.Lakehouse -P enableSchemas=true
# MAGIC fab create OLS_Marketing_.Workspace/OLS_Marketing_LH.Lakehouse -P enableSchemas=true
# MAGIC fab create OLS_Sales_.Workspace/OLS_Sales_LH.Lakehouse -P enableSchemas=true


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Step 4: Upload the data & create Lakehouse tables (manual) 
# 1. Open up `OLS_Gold_.Workspace/OLS_Gold_LH.Lakehouse`
# 2. Download the CSVs from the Skool page, and upload them into the Files/ area of the Lakehouse
# 3. Convert the files into OneLake tables, for now just do it through the "Load to Tables" functionality. 
# 


# MARKDOWN ********************

# #### Step 5: Run this cell to create shortcuts across the three Lakehouses

# CELL ********************

# MAGIC %%sh
# MAGIC GOLD="OLS_Gold_.Workspace/OLS_Gold_LH.Lakehouse/Tables/dbo"
# MAGIC SALES="OLS_Sales_.Workspace/OLS_Sales_LH.Lakehouse/Tables/dbo"
# MAGIC MKTG="OLS_Marketing_.Workspace/OLS_Marketing_LH.Lakehouse/Tables/dbo"
# MAGIC 
# MAGIC # Shared -> both downstream workspaces
# MAGIC for t in dim_date dim_geography dim_product dim_customer fact_subscription; do
# MAGIC   fab ln $SALES/$t.Shortcut --type oneLake --target $GOLD/$t -f
# MAGIC   fab ln $MKTG/$t.Shortcut  --type oneLake --target $GOLD/$t -f
# MAGIC done
# MAGIC 
# MAGIC # Sales-only 
# MAGIC for t in fact_usage fact_revenue; do
# MAGIC   fab ln $SALES/$t.Shortcut --type oneLake --target $GOLD/$t -f
# MAGIC done
# MAGIC 
# MAGIC # Marketing-only
# MAGIC for t in fact_campaign dim_channel; do
# MAGIC   fab ln $MKTG/$t.Shortcut --type oneLake --target $GOLD/$t -f
# MAGIC done

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
