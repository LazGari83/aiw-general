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

# ## ING0XX 🔶 Open Mirroring
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute.
# 
# In this exercise, we're going to explore a Fabric feature called Open Mirroring. 
# 
# With Open Mirroring, you just need to write CSV or Parquet files into a Landing Zone folder, and the files will be automatically replicated into a managed Delta table in OneLake. 
# 
# We're going to explore: 
# - Part 1: Understanding the fundamentals
# - Part 2: Parquet files
# - Part 3: Schemas 
# - Reflections discussing implementation strategies (this part is in [Skool page])
# 
# Let's dive in! 
# 
# #### Prerequisites
# 1. In a Fabric Workspace, click New Item, and create a new 'Mirrored Database'. 
# 2. Copy the Landing zone URL from the Home page of the mirrored database — you'll need to paste it into the 'Setup' cell below. 
# 3. Download this notebook from Skool, and import it into your Fabric workspace, and run through it cell-by-cell. 


# MARKDOWN ********************

# #### Helper functions
# 
# Imports and defining some helper functions, which we will use throughout the notebook. 
# 
# Important: you'll need to update `landzone_url` to match the URL of your own Mirrored Database.
# 
# I've added some comments to explain what each of these does. 
# - the writing functions use notebookutils, and therefore will only run inside Fabric. If you want to write files from external systems, you'll need to use a service principal, [see an example here](https://github.com/UnifiedEducation/research/blob/main/open-mirroring/clients/auth.py).

# CELL ********************

import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import notebookutils

landzone_url = "https://onelake.dfs.fabric.microsoft.com/304b1974-676d-48af-b4e4-8cf6f0fe139e/4c9a7612-bc15-47cc-b724-9d05978c72d2/Files/LandingZone"


def lz_filename(n: int, ext: str) -> str:
    """This func generates a filename (with zero-padding, up to 20 chars). 
    Essentially a format like '00000000000000000001.csv' - because Open Mirroring uses lexical sorting
    """
    return f"{n:020d}.{ext}"


def write_metadata(table: str, meta: dict, schema: str | None = None) -> str:
    """This func writes the _metadata.json file needed for each table in Open Mirroring
    Handles both with / without schemas. 
    """
    folder = f"{schema}.schema/{table}" if schema else table
    path = f"{landzone_url}/{folder}/_metadata.json"
    notebookutils.fs.put(path, json.dumps(meta, indent=2), True)
    return path


def upload_csv(table: str, n: int, df: pd.DataFrame, schema: str | None = None) -> str:
    """This func writes a CSV to the landing zone. 
    """
    folder = f"{schema}.schema/{table}" if schema else table
    path = f"{landzone_url}/{folder}/{lz_filename(n, 'csv')}"
    notebookutils.fs.put(path, df.to_csv(index=False, lineterminator="\n"), True)
    return path


def upload_parquet(table: str, n: int, df: pd.DataFrame, pa_schema: pa.Schema, schema: str | None = None) -> str:
    """This func writes a CSV to the landing zone. 
    """
    arrays = [pa.array(df[f.name].tolist(), type=f.type) for f in pa_schema]
    tbl = pa.Table.from_arrays(arrays, schema=pa_schema)
    local = f"/tmp/{lz_filename(n, 'parquet')}"
    pq.write_table(tbl, local)
    folder = f"{schema}.schema/{table}" if schema else table
    path = f"{landzone_url}/{folder}/{lz_filename(n, 'parquet')}"
    notebookutils.fs.cp(f"file://{local}", path, True)
    return path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ---
# 
# ## Part 1: Understanding the fundamentals
# 
# In this section, I'll walk through some of the fundamentals to get Open Mirroring working - specifically: 
# - understanding the Landing Zone
# - understanding the role of _metadata.json and how to configure it. 
# - understanding the required structure of your CSV / Parquet. 


# MARKDOWN ********************

# #### Task 1.1: Understand the landing zone
# 
# As you should have seen by now, when you create a Mirrored Database, you should see your Landing Zone URL, which is auto-provisioned. 
# 
# It should be of this structure: 
# 
# ```
# https://onelake.dfs.fabric.microsoft.com/<workspaceId>/<mirroredDatabaseId>/Files/LandingZone/
# ```
# 
# This should look familiar! Essentially it's just a Lakehouse, which is tightly controlled, and has some listeners targeting the "Files/LandingZone" folder - when you drop files in here, there are automatically saved to the 'Tables/' part of the Mirrored database too. 
# 
# We can use notebookutils (inside Fabric only), to list the resources in that folder (although it will probably be empty currently): 

# CELL ********************

notebookutils.fs.ls(landzone_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 1.2: Understand the `_metadata.json` file 
# 
# Each table / dataset you want to mirror must have it's own folder under Files/ (or under the Schema, if using them). 
# 
# In the folder for each table, you are required to write a `_metadata.json` file. 
# 
# The required structure is different if you're writing Parquet or CSV data into the folder: 
# - For Parquet, the _metadata.json is pretty simple, just need one property {"keyColumns":["c1","c2"]} - this represents the Key column (or columns), which are used for update / upsert / delete operations. 
# - if you're writing CSVs, the `_metadata.json` required more: you need to add the schema of the CSV files you're writing, because CSV is just text and has no embedded type information.
# 
# Here's a look at what we're aiming for here:
# 
# ```
# Files/                  <- Files root
#     employees/          <- table / dataset name
#       000000001.csv     <- this is our CSV file written to the employees folder (we'll write this a bit later on)
#       _metadata.json    <- the metadata file describing the schema & key columns in the dataset
# ```


# CELL ********************

write_metadata("employees", {
    "keyColumns": ["EmployeeID"],
    "FileFormat": "DelimitedText",
    "FileExtension": "csv",
    "SchemaDefinition": {
        "Columns": [
            {"Name": "EmployeeID", "DataType": "String"},
            {"Name": "EmployeeLocation", "DataType": "String", "IsNullable": True},
        ]
    },
    "FileFormatTypeProperties": {
        "FirstRowAsHeader": True, "RowSeparator": "\n",
        "ColumnSeparator": ",",
        "Encoding": "UTF-8",
    },
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Each top-level key in the metadata file plays a specific role:
# - `keyColumns`: Required for updates, deletes, upserts. Names the column(s) that uniquely identify a row. 
# - `FileFormat`: Set to "DelimitedText" for CSV/TSV. Omit for Parquet (the default). 
# - `FileExtension`: Required when FileFormat is DelimitedText. Tells mirroring which file extension to look for in the folder. 
# - `SchemaDefinition`: Required for delimited text - declares column names, types and nullability. Not needed for Parquet (Parquet self-describes). 
# - `FileFormatTypeProperties`: Optional knobs for delimited text - column separator, row separator, quote character, encoding, null spelling, etc. 
# 
# Note: `keyColumns` can be added at any time, but once set it cannot be changed.

# MARKDOWN ********************

# #### Task 1.3: Initial load your first file 
# 
# As we'll see as we go forward, a defining feature of Open Mirroring is the requirements for a `__rowMarker__` column to be added at the end of your dataset. This column tells the Mirrored ingestion engine whether the row is an insert, update, delete, or upsert. We'll explore these more in the next section. 
# 
# For now, it's important to know that for an initial batch load (i.e the first file you upload), you don't need to include a `__rowMarker__` column. In fact if you've got a large dataset, it's recommended you don't add it - the engine will treat it as a bulk insert. 
# 
# Run the following cell to insert three rows to `employees`, then open up your mirrored database in the Fabric portal and watch the `employees` table appear under Tables - you'll need to keep refreshing, it can sometimes take a minute for the UI to update and/or the SQL Endpoint to refresh (if that's where you're looking at it). 

# CELL ********************

df = pd.DataFrame([
    {"EmployeeID": "E0001", "EmployeeLocation": "Redmond"},
    {"EmployeeID": "E0002", "EmployeeLocation": "Redmond"},
    {"EmployeeID": "E0003", "EmployeeLocation": "Redmond"},
])
upload_csv("employees", 1, df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Understanding the file structure (and `__rowMarker__`)
# 
# Once you want to write a second file to your landing zone folder, the file needs to have the `__rowMarker__` column. 
# 
# There are four possible values you can add for each row in this column, it dictates what the mirroring ingestion engine handles each row: 
# 
# | `__rowMarker__` | Meaning |
# | --- | --- |
# | `0` | Insert row |
# | `1` | Update row |
# | `2` | Delete row |
# | `4` | Upsert (insert-or-update) |
# 
# Really important: **`__rowMarker__` must be the last column** in your file.
# 
# Let's look at some examples...
# 
# #### Task 1.4: (`__rowMarker__ : 0`)
# 
# This cell adds a new employee with `__rowMarker__ : 0` (insert).

# CELL ********************

df = pd.DataFrame([
    {"EmployeeID": "E0004", "EmployeeLocation": "Seattle", "__rowMarker__": 0},
])
upload_csv("employees", 2, df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 1.5: Update (`__rowMarker__ : 1`)
# 
# For an update, you must include all columns in the row - not just the columns that changed. The key column(s) identify the target row; the other columns overwrite the existing values.
# 
# This cell updates the EmployeeLocation of Employee E0001 from Redmond to Bellevue.

# CELL ********************

df = pd.DataFrame([
    {"EmployeeID": "E0001", "EmployeeLocation": "Bellevue", "__rowMarker__": 1},
])
upload_csv("employees", 3, df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 1.6: Delete (`__rowMarker__ : 2`)
# 
# For a delete, only the key columns are required - other columns can be empty. In CSV, "empty" means an empty cell between two delimiters (the default null spelling, controlled by `NullValue` in `FileFormatTypeProperties`).
# 
# Delete E0002.

# CELL ********************

df = pd.DataFrame([
    {"EmployeeID": "E0002", "EmployeeLocation": None, "__rowMarker__": 2},
])
upload_csv("employees", 4, df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 1.7: Upsert (`__rowMarker__ : 4`)
# 
# `upsert` inserts the row if the key doesn't already exist in the table, or updates the row if it does.
# 
# This cell upserts two rows - one new (E0005), one existing (E0003).

# CELL ********************

df = pd.DataFrame([
    {"EmployeeID": "E0005", "EmployeeLocation": "Portland", "__rowMarker__": 4},
    {"EmployeeID": "E0003", "EmployeeLocation": "Kirkland", "__rowMarker__": 4},
])
upload_csv("employees", 5, df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ---
# 
# ## Part 2: Parquet files
# 
# An Open Mirroring Landing Zone also accepts Parquet files. If you're considering whether to write CSV or Parquet, and you have that choice, I would recommend choosing Parquet, it's much simpler and more efficient too. 
# 
# When writing Parquet files to the landing zone, the process is pretty same identical, the only difference is the _metadata.json file is a lot simpler- only keyColumns property is required. 
# 
# Let's write a second table metadata, this time for 'products' 


# MARKDOWN ********************

# #### Task 2.1: `_metadata.json` for Parquet files


# CELL ********************

write_metadata("products", {"keyColumns": ["ProductID"]})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 2.2: Initial load as Parquet
# 
# Same as CSV initial load - the first file omits `__rowMarker__` column and is treated as a bulk insert.
# 
# Note, we use PyArrow to create the Parquet file, and declare a PyArrow schema (pa.schema()) to make the data types explicit. A word of warning though, you can't change data types after you run the first ingestion for a given table - it needs to remain constant. If you need to change the data type, you'll need to remove the whole folder and recreate it with the new / correct data types. 

# CELL ********************

products_schema = pa.schema([
    ("ProductID", pa.string()),
    ("ProductName", pa.string()),
    ("Price", pa.float64()),
])

df = pd.DataFrame([
    {"ProductID": "P001", "ProductName": "Widget", "Price": 9.99},
    {"ProductID": "P002", "ProductName": "Gadget", "Price": 14.50},
])
upload_parquet("products", 1, df, products_schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 2.3: Updating a row using `__rowMarker__ : 1` (and Parquet file format)


# CELL ********************

products_schema_with_marker = pa.schema([
    ("ProductID", pa.string()),
    ("ProductName", pa.string()),
    ("Price", pa.float64()),
    ("__rowMarker__", pa.int32()),
])

df = pd.DataFrame([
    {"ProductID": "P001", "ProductName": "Widget", "Price": 11.99, "__rowMarker__": 1},
])
upload_parquet("products", 2, df, products_schema_with_marker)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 2.4: When to choose which
# 
# Both formats produce **identical Delta tables** on the destination side. The choice is about what's easier for your *publisher*:
# 
# | | CSV | Parquet |
# | --- | --- | --- |
# | **Inspectability** | ✅ Open in any text editor | ❌ Needs a Parquet reader |
# | **File size** | Larger | Smaller (columnar + compression) |
# | **Read speed** | Slower | Faster |
# | **Schema management** | Declared once in metadata | Declared per file in pyarrow |
# | **Type safety** | Schema enforced by metadata | Schema enforced by pyarrow |
# | **Library required** | None (any language can write CSV) | Parquet writer (pyarrow, fastparquet, parquet-mr, etc.) |
# 
# Rule of thumb:
# - **Demos, low volume, human-curated data, quick debugging** → CSV.
# - **Production volumes, machine-to-machine pipelines, performance-sensitive workloads** → Parquet.

# MARKDOWN ********************

# ---
# 
# ## Part 3: Schemas (organising the landing zone)
# 
# Up until this point, we've been creating table folders directly under the 'Files/LandingZone/' root - this creates delta tables in the default schema of the Mirrored Database (normally `dbo`). 
# 
# In practice, it's likely you'll want to sub-divide your Landing Zone, by Source System. The way to do that is to use Schemas within the Landing Zone, which are then respected when the tables are created on the Mirrored Database side. 
# 
# You'll need to write to a folder, using this naming convention:
# 
# 
# ```
# …/Files/LandingZone/<schemaname>.schema/<tablename>/<file>
# ```
# The `.schema` suffix on the folder name is the important bit. 
# 
# Let's create a third folder & table, this time within a schema. 

# MARKDOWN ********************

# #### Task 3.1: The `<schema>.schema/` folder convention
# 
# The path shape for a schema-qualified table is:
# 
# ```
# …/Files/LandingZone/<schemaname>.schema/<tablename>/<file>
# ```
# 


# CELL ********************

notebookutils.fs.ls(landzone_url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Task 3.1: Create a schema and a table inside it
# 
# New table: `orders` inside a schema called `sales`. 
# 
# Once mirroring picks it up, the table will appear in the mirrored database SQL endpoint as `sales.orders` — fully schema-qualified.

# CELL ********************

write_metadata("orders", {
    "keyColumns": ["OrderID"],
    "FileFormat": "DelimitedText",
    "FileExtension": "csv",
    "SchemaDefinition": {
        "Columns": [
            {"Name": "OrderID", "DataType": "String"},
            {"Name": "CustomerID", "DataType": "String"},
            {"Name": "Amount", "DataType": "Double"},
        ]
    },
    "FileFormatTypeProperties": {"FirstRowAsHeader": True, "RowSeparator": "\n", "ColumnSeparator": ","},
}, schema="sales")

df = pd.DataFrame([
    {"OrderID": "O001", "CustomerID": "C100", "Amount": 49.99},
    {"OrderID": "O002", "CustomerID": "C101", "Amount": 149.50},
    {"OrderID": "O003", "CustomerID": "C100", "Amount": 12.00},
])
upload_csv("orders", 1, df, schema="sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Wrap-up 
# That's it for the fundamentals - head back to the [Skool page](https://www.skool.com/fabricdojo/classroom/b2e43d21?md=4826838a6b704d15b96403da0a6b17fa) to continue with the Reflections. 
# 

