# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "f8c7458c-3668-4d31-846c-42e860629b01",
# META       "default_lakehouse_name": "LH017_Essentials_Rui",
# META       "default_lakehouse_workspace_id": "304b1974-676d-48af-b4e4-8cf6f0fe139e",
# META       "known_lakehouses": [
# META         {
# META           "id": "f8c7458c-3668-4d31-846c-42e860629b01"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # LH021 🔶 Complex Types: Arrays, Maps, Structs and Explode
# ## Fundamentals Module - Lesson 5
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the fifth lesson in our Fundamentals module! In this lesson, we'll explore complex data types in Apache Spark - Arrays, Maps, and Structs - which are essential for working with nested and semi-structured data.
# 
# ### What you'll learn in this lesson:
# - **Arrays, Maps, and Structs** - Understanding each complex type and when to use them
# - **Explode and Posexplode** - Flattening nested data structures
# - **Accessing Nested Fields** - Navigating through complex hierarchies
# - **Array Functions** - Using transform, filter, and aggregate
# - **Working with JSON** - Parsing and creating JSON structures
# - **Read and Write without losing schema** - Preserving complex types in storage
# 
# ### Prerequisites:
# - Basic understanding of DataFrames in Spark
# - Familiarity with basic Spark operations
# - Understanding of data types in programming


# MARKDOWN ********************

# ## Setting Up Our Environment
# 
# Let's start by importing the necessary libraries that we'll use throughout this lesson.

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *

print("Libraries imported successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Understanding Complex Types
# 
# Spark supports three main complex types that allow you to work with nested and semi-structured data:
# 
# ### Complex Types Overview:
# - **ArrayType**: An ordered collection of elements of the same type (like a list)
# - **MapType**: A collection of key-value pairs (like a dictionary)
# - **StructType**: A collection of named fields with potentially different types (like a row or object)
# 
# These types are essential when working with:
# - JSON data
# - Denormalized data models
# - Data with variable-length attributes
# - Hierarchical data structures

# MARKDOWN ********************

# ### Creating Sample Datasets with Complex Types
# 
# Let's create realistic datasets that demonstrate each complex type. We'll use an e-commerce scenario with:
# - **Customers**: With arrays of phone numbers and interests
# - **Products**: With maps for specifications
# - **Orders**: With arrays of order items (structs)

# CELL ********************

# Create a DataFrame with Arrays
# Arrays are useful for storing multiple values of the same type

customers_data = [
    (1, "Alice Johnson", ["555-0101", "555-0102"], ["electronics", "books", "sports"]),
    (2, "Bob Smith", ["555-0201"], ["home", "garden"]),
    (3, "Carol Davis", ["555-0301", "555-0302", "555-0303"], ["fashion", "beauty"]),
    (4, "David Wilson", [], ["electronics"]),
    (5, "Emma Brown", ["555-0501"], [])
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("phone_numbers", ArrayType(StringType()), True),
    StructField("interests", ArrayType(StringType()), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

print("Customers DataFrame with Arrays:")
customers_df.show(truncate=False)
customers_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a DataFrame with Structs
# Structs are useful for grouping related fields together

customers_with_address_data = [
    (1, "Alice Johnson", ("123 Main St", "New York", "NY", "10001")),
    (2, "Bob Smith", ("456 Oak Ave", "Los Angeles", "CA", "90001")),
    (3, "Carol Davis", ("789 Pine Rd", "Chicago", "IL", "60601")),
    (4, "David Wilson", ("321 Elm Blvd", "Houston", "TX", "77001")),
    (5, "Emma Brown", ("654 Cedar Ln", "Phoenix", "AZ", "85001"))
]

address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True)
])

customers_address_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("address", address_schema, True)
])

customers_address_df = spark.createDataFrame(customers_with_address_data, customers_address_schema)

print("Customers DataFrame with Struct (Address):")
customers_address_df.show(truncate=False)
customers_address_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a DataFrame with Maps
# Maps are useful for key-value pairs where keys might vary between records

products_data = [
    (101, "Laptop Pro", {"brand": "TechCo", "RAM": "16GB", "storage": "512GB SSD"}),
    (102, "Wireless Mouse", {"brand": "ClickMaster", "color": "black", "battery": "AA"}),
    (103, "Office Chair", {"brand": "ComfortPlus", "material": "mesh", "weight_capacity": "300lbs"}),
    (104, "Coffee Maker", {"brand": "BrewBest", "capacity": "12 cups"}),
    (105, "Desk Lamp", {"brand": "LightUp", "wattage": "60W", "color": "white"})
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("specifications", MapType(StringType(), StringType()), True)
])

products_df = spark.createDataFrame(products_data, products_schema)

print("Products DataFrame with Map (Specifications):")
products_df.show(truncate=False)
products_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a DataFrame with Array of Structs (common pattern for order items)

orders_data = [
    (1001, 1, "2024-01-15", [
        (101, "Laptop Pro", 1, 999.99),
        (102, "Wireless Mouse", 2, 29.99)
    ]),
    (1002, 2, "2024-01-16", [
        (104, "Coffee Maker", 1, 79.99)
    ]),
    (1003, 3, "2024-01-18", [
        (103, "Office Chair", 1, 199.99),
        (105, "Desk Lamp", 2, 49.99),
        (102, "Wireless Mouse", 1, 29.99)
    ]),
    (1004, 1, "2024-01-20", [
        (105, "Desk Lamp", 1, 49.99)
    ])
]

item_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True)
])

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("items", ArrayType(item_schema), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

print("Orders DataFrame with Array of Structs:")
orders_df.show(truncate=False)
orders_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 2: Accessing Nested Fields
# 
# Now that we have complex types, let's learn how to access the data within them.
# 
# ### Access Patterns:
# - **Struct fields**: Use dot notation `column.field` or `col("column.field")`
# - **Array elements**: Use `getItem(index)` or bracket notation `column[index]`
# - **Map values**: Use `getItem(key)` or bracket notation `column[key]`

# CELL ********************

# Accessing Struct fields using dot notation

print("Accessing Struct fields:")
print("="*50)

customers_address_df.printSchema()

# Method 1: Using dot notation in select
customers_address_df.select(
    "name",
    "address.street",
    "address.city",
    "address.state"
).show(truncate=False)

# Method 2: Using col() function
customers_address_df.select(
    col("name"),
    col("address.city").alias("city"),
    col("address.zip_code").alias("zip")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Accessing Array elements

print("Accessing Array elements:")
print("="*50)

customers_df.printSchema()

# Get first element (index 0)
customers_df.select(
    "name",
    col("phone_numbers")[0].alias("primary_phone"),
    col("interests")[0].alias("first_interest")
).show(truncate=False)

# Using getItem() method
customers_df.select(
    "name",
    col("phone_numbers").getItem(0).alias("primary_phone"),
    size("phone_numbers").alias("num_phones")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Accessing Map values

print("Accessing Map values:")
print("="*50)

products_df.printSchema()

# Get value by key
products_df.select(
    "product_name",
    col("specifications")["brand"].alias("brand"),
    col("specifications").getItem("color").alias("color")
).show(truncate=False)

# Get all keys and values from a map
products_df.select(
    "product_name",
    map_keys("specifications").alias("spec_keys"),
    map_values("specifications").alias("spec_values")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Accessing nested structures (Array of Structs)

print("Accessing Array of Structs:")
print("="*50)

orders_df.printSchema()

# Get first item's details
orders_df.select(
    "order_id",
    col("items")[0].alias("first_item"),
    col("items")[0]["product_name"].alias("first_product"),
    col("items")[0]["quantity"].alias("first_qty")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 3: Explode and Posexplode
# 
# The `explode` function is one of the most important tools for working with complex types. It transforms array or map columns into multiple rows.
# 
# ### Explode Functions:
# - **explode()**: Creates a new row for each element in an array or each key-value pair in a map
# - **explode_outer()**: Same as explode, but preserves rows with NULL or empty arrays/maps
# - **posexplode()**: Same as explode, but also returns the position (index) of each element
# - **posexplode_outer()**: Combination of posexplode and explode_outer

# CELL ********************

# Basic explode on arrays

print("Basic explode - Phone Numbers:")
print("="*50)

print("Pre-explode:")
customers_df.show()

exploded_phones = customers_df.select(
    "customer_id",
    "name",
    explode("phone_numbers").alias("phone")
)

print("Pos-explode:")
exploded_phones.show(truncate=False)

print("Notice: David (empty array) is missing from results!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# explode_outer preserves rows with empty arrays

print("explode_outer - Preserves empty arrays:")
print("="*50)

print("Pre-explode:")
customers_df.show()

exploded_phones_outer = customers_df.select(
    "customer_id",
    "name",
    explode_outer("phone_numbers").alias("phone")
)

print("Pos-explode:")

exploded_phones_outer.show(truncate=False)

print("Notice: David now appears with NULL for phone!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# posexplode includes the position/index

print("posexplode - With position index:")
print("="*50)

print("Pre-explode:")
customers_df.show()

posexploded_phones = customers_df.select(
    "customer_id",
    "name",
    posexplode("phone_numbers").alias("position", "phone")
)

print("Pos-explode:")

posexploded_phones.show(truncate=False)

print("The position column shows the index of each element (0-based)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode on Maps

print("Explode on Maps:")
print("="*50)

print("Pre-explode:")
products_df.show()

exploded_specs = products_df.select(
    "product_id",
    "product_name",
    explode("specifications").alias("spec_name", "spec_value")
)

print("Pos-explode:")
exploded_specs.show(truncate=False)

print("Each key-value pair becomes a separate row!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Explode Array of Structs - Very common pattern!

print("Explode Array of Structs (Order Items):")
print("="*50)

print("Pre-explode:")
orders_df.show()

exploded_orders = orders_df.select(
    "order_id",
    "customer_id",
    "order_date",
    explode("items").alias("item")
)

print("Pos-explode:")

exploded_orders.show(truncate=False)

# Now we can access the struct fields
print("With struct fields extracted:")
exploded_orders.select(
    "order_id",
    "customer_id",
    "order_date",
    col("item.product_id"),
    col("item.product_name"),
    col("item.quantity"),
    col("item.unit_price"),
    (col("item.quantity") * col("item.unit_price")).alias("line_total")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 4: Array Functions - Transform, Filter, Aggregate
# 
# Spark provides powerful higher-order functions to manipulate arrays without exploding them.
# 
# ### Key Array Functions:
# - **transform()**: Apply a function to each element
# - **filter()**: Keep only elements that match a condition
# - **aggregate()**: Reduce array to a single value
# - **exists()**: Check if any element matches a condition
# - **forall()**: Check if all elements match a condition

# CELL ********************

# Transform - Apply a function to each array element

print("Transform - Modify each element:")
print("="*50)

# Convert all interests to uppercase
customers_df.select(
    "name",
    "interests",
    transform("interests", lambda x: upper(x)).alias("interests_upper")
).show(truncate=False)

# Add a prefix to phone numbers
customers_df.select(
    "name",
    "phone_numbers",
    transform("phone_numbers", lambda x: concat(lit("+1-"), x)).alias("phones_formatted")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Filter - Keep only elements matching a condition

print("Filter - Keep matching elements:")
print("="*50)

# Keep only interests starting with 'e'
customers_df.select(
    "name",
    "interests",
    filter("interests", lambda x: x.startswith("e")).alias("e_interests")
).show(truncate=False)

# Keep phone numbers containing '02'
customers_df.select(
    "name",
    "phone_numbers",
    filter("phone_numbers", lambda x: x.contains("02")).alias("phones_with_02")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Aggregate - Reduce array to single value

print("Aggregate - Reduce to single value:")
print("="*50)

# Calculate total price from order items
orders_df.select(
    "order_id",
    "items",
    aggregate(
        "items",
        lit(0.0).cast("double"),
        lambda acc, x: acc + (x.quantity * x.unit_price)
    ).alias("order_total")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exists and Forall - Boolean checks on arrays

print("Exists and Forall - Boolean checks:")
print("="*50)

# Check if customer has interest in 'electronics'
customers_df.select(
    "name",
    "interests",
    exists("interests", lambda x: x == "electronics").alias("likes_electronics")
).show(truncate=False)

# Check if all phone numbers start with '555'
customers_df.select(
    "name",
    "phone_numbers",
    forall("phone_numbers", lambda x: x.startswith("555")).alias("all_start_555")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Other useful array functions

print("Other Useful Array Functions:")
print("="*50)

customers_df.select(
    "name",
    "interests",
    size("interests").alias("num_interests"),
    array_contains("interests", "books").alias("likes_books"),
    array_distinct("interests").alias("unique_interests"),
    array_sort("interests").alias("sorted_interests")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 5: Working with JSON
# 
# JSON is one of the most common data formats, and Spark provides excellent support for parsing and creating JSON structures.
# 
# ### Key JSON Functions:
# - **from_json()**: Parse JSON string into struct/array
# - **to_json()**: Convert struct/array to JSON string
# - **get_json_object()**: Extract value using JSON path
# - **json_tuple()**: Extract multiple values from JSON

# CELL ********************

# Create a DataFrame with JSON strings

json_data = [
    (1, '{"name": "Alice", "age": 30, "city": "New York"}'),
    (2, '{"name": "Bob", "age": 25, "city": "Los Angeles"}'),
    (3, '{"name": "Carol", "age": 35, "city": "Chicago"}'),
    (4, '{"name": "David", "age": 28, "city": "Houston"}')
]

json_df = spark.createDataFrame(json_data, ["id", "json_string"])

print("DataFrame with JSON strings:")
json_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Parsing JSON with from_json()

print("Parsing JSON with from_json():")
print("="*50)

# Define the schema for the JSON
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("city", StringType(), True)
])

# Parse JSON string into struct
parsed_df = json_df.select(
    "id",
    from_json("json_string", json_schema).alias("parsed_data")
)

parsed_df.show(truncate=False)
parsed_df.printSchema()

# Now we can access the fields
print("Accessing parsed fields:")
parsed_df.select(
    "id",
    "parsed_data.name",
    "parsed_data.age",
    "parsed_data.city"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using get_json_object() for simple extractions

print("Using get_json_object():")
print("="*50)

# Extract values using JSON path
json_df.select(
    "id",
    get_json_object("json_string", "$.name").alias("name"),
    get_json_object("json_string", "$.age").alias("age"),
    get_json_object("json_string", "$.city").alias("city")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converting structs back to JSON with to_json()

print("Converting back to JSON with to_json():")
print("="*50)

customers_address_df.show()

# Convert our customers with address back to JSON
customers_address_df.select(
    "customer_id",
    "name",
    to_json("address").alias("address_json")
).show(truncate=False)

# Convert the entire row to JSON
customers_address_df.select(
    to_json(struct("*")).alias("full_json")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Working with nested JSON

nested_json_data = [
    (1, '{"customer": {"name": "Alice", "email": "alice@email.com"}, "items": [{"product": "Laptop", "price": 999.99}, {"product": "Mouse", "price": 29.99}]}'),
    (2, '{"customer": {"name": "Bob", "email": "bob@email.com"}, "items": [{"product": "Chair", "price": 199.99}]}')
]

nested_json_df = spark.createDataFrame(nested_json_data, ["order_id", "json_data"])

print("Nested JSON data:")
nested_json_df.show(truncate=False)

# Define nested schema
nested_item_schema = StructType([
    StructField("product", StringType(), True),
    StructField("price", DoubleType(), True)
])

nested_customer_schema = StructType([
    StructField("name", StringType(), True),
    StructField("email", StringType(), True)
])

nested_schema = StructType([
    StructField("customer", nested_customer_schema, True),
    StructField("items", ArrayType(nested_item_schema), True)
])

# Parse nested JSON
parsed_nested = nested_json_df.select(
    "order_id",
    from_json("json_data", nested_schema).alias("data")
)

print("Parsed nested structure:")
parsed_nested.printSchema()

# Access nested fields and explode items
print("Extracted and exploded:")
parsed_nested.select(
    "order_id",
    col("data.customer.name").alias("customer_name"),
    explode("data.items").alias("item")
).select(
    "order_id",
    "customer_name",
    "item.product",
    "item.price"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 6: Read and Write Without Losing Schema
# 
# One of the challenges with complex types is preserving the schema when saving and loading data.
# 
# ### File Format Comparison:
# - **Parquet**: Best choice - full support for all complex types, schema is embedded
# - **Delta**: Excellent support (built on Parquet) with additional features
# - **JSON**: Good support but schema must be inferred or provided on read
# - **CSV**: Poor support - complex types are converted to strings

# CELL ********************

# Saving complex types to Parquet (recommended)

print("Saving to Parquet (preserves all complex types):")
print("="*50)

# Save our orders DataFrame with Array of Structs
orders_df.write.mode("overwrite").parquet("Files/complex_types_demo/orders_parquet")

# Read it back
orders_from_parquet = spark.read.parquet("Files/complex_types_demo/orders_parquet")

print("Schema after reading from Parquet:")
orders_from_parquet.printSchema()
orders_from_parquet.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Saving to Delta format

print("Saving to Delta (with schema evolution support):")
print("="*50)

# Save to Delta
orders_df.write.mode("overwrite").format("delta").save("Files/complex_types_demo/orders_delta")

# Read it back
orders_from_delta = spark.read.format("delta").load("Files/complex_types_demo/orders_delta")

print("Schema after reading from Delta:")
orders_from_delta.printSchema()
orders_from_delta.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Saving to JSON (schema must be inferred on read)

print("Saving to JSON:")
print("="*50)

# Save to JSON
orders_df.write.mode("overwrite").json("Files/complex_types_demo/orders_json")

# Read it back - Spark will infer the schema
orders_from_json = spark.read.json("Files/complex_types_demo/orders_json")

print("Schema after reading from JSON (auto-inferred):")
orders_from_json.printSchema()
orders_from_json.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading JSON with explicit schema (recommended for production)

print("Reading JSON with explicit schema:")
print("="*50)

# Define the schema explicitly
explicit_item_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True)
])

explicit_orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("items", ArrayType(explicit_item_schema), True)
])

# Read with explicit schema - faster and more reliable
orders_with_schema = spark.read.schema(explicit_orders_schema).json("Files/complex_types_demo/orders_json")

print("Schema with explicit definition:")
orders_with_schema.printSchema()
orders_with_schema.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Demonstrating the problem with CSV and complex types

print("CSV and Complex Types (NOT recommended):")
print("="*50)

# Try to save arrays to CSV - Error on Array Types!
customers_df.write.mode("overwrite").option("header", True).csv("Files/complex_types_demo/customers_csv")

# # Read it back
# customers_from_csv = spark.read.option("header", True).csv("Files/complex_types_demo/customers_csv")

# print("Schema after reading from CSV:")
# customers_from_csv.printSchema()
# customers_from_csv.show(truncate=False)

# print("\nNotice: Arrays are now strings! You would need to parse them manually.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Best Practices for Preserving Complex Types:
# 
# 1. **Use Parquet or Delta** as your primary storage format for complex types
# 2. **Provide explicit schemas** when reading JSON to ensure consistency
# 3. **Avoid CSV** for data with complex types
# 4. **Document your schemas** for team reference
# 5. **Use schema evolution** features in Delta when your data structure changes

# MARKDOWN ********************

# ## Part 7: Creating Complex Types from Simple Columns
# 
# Sometimes you need to create complex types from existing simple columns. Spark provides functions for this.

# CELL ********************

# Create a simple DataFrame

simple_data = [
    (1, "Alice", "555-0101", "New York", "NY", "10001"),
    (2, "Bob", "555-0201", "Los Angeles", "CA", "90001"),
    (3, "Carol", "555-0301", "Chicago", "IL", "60601")
]

simple_df = spark.createDataFrame(simple_data, 
    ["id", "name", "phone", "city", "state", "zip"])

print("Simple DataFrame:")
simple_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating Structs from columns using struct()

print("Creating Struct from columns:")
print("="*50)

with_struct = simple_df.select(
    "id",
    "name",
    "phone",
    struct(
        col("city"),
        col("state"),
        col("zip").alias("zip_code")
    ).alias("address")
)

with_struct.show(truncate=False)
with_struct.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating Arrays from columns using array()

print("Creating Array from columns:")
print("="*50)

with_array = simple_df.select(
    "id",
    "name",
    array(col("city"), col("state"), col("zip")).alias("location_parts")
)

with_array.show(truncate=False)
with_array.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Creating Maps using create_map()

print("Creating Map from columns:")
print("="*50)

with_map = simple_df.select(
    "id",
    "name",
    create_map(
        lit("city"), col("city"),
        lit("state"), col("state"),
        lit("zip"), col("zip")
    ).alias("location_map")
)

with_map.show(truncate=False)
with_map.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Converting arrays to strings and back

print("Array to String and back:")
print("="*50)

# Array to string using concat_ws (with separator)
array_to_string = customers_df.select(
    "name",
    "phone_numbers",
    concat_ws(", ", "phone_numbers").alias("phones_string")
)
array_to_string.show(truncate=False)

# String to array using split()
print("String to Array using split():")
string_to_array = array_to_string.select(
    "name",
    "phones_string",
    split("phones_string", ", ").alias("phones_array_again")
)
string_to_array.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Practical Exercises
# 
# Now it's your turn to practice! Complete the following exercises using the concepts learned.

# MARKDOWN ********************

# ### Exercise 1: Working with Arrays
# 
# Using the **customers_df** DataFrame:
# 1. Find all customers who have more than 1 phone number
# 2. Count the total number of interests across all customers
# 3. Find customers who are interested in both 'electronics' and 'books'

# CELL ********************

# Exercise 1.1: Find customers with more than 1 phone number

exercise_1_1 = customers_df.filter(size("phone_numbers") > 1) \
    .select("name", "phone_numbers", size("phone_numbers").alias("phone_count"))

print("Exercise 1.1 - Customers with > 1 phone:")
exercise_1_1.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.2: Count total interests across all customers

exercise_1_2 = customers_df.select(
    sum(size("interests")).alias("total_interests")
)

print("Exercise 1.2 - Total interests count:")
exercise_1_2.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.3: Find customers interested in both electronics AND books

exercise_1_3 = customers_df.filter(
    array_contains("interests", "electronics") & 
    array_contains("interests", "books")
).select("name", "interests")

print("Exercise 1.3 - Customers interested in electronics AND books:")
exercise_1_3.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 2: Explode and Aggregate
# 
# Using the **orders_df** DataFrame:
# 1. Explode the orders to show one row per item
# 2. Calculate the total revenue per product
# 3. Find the average number of items per order

# CELL ********************

# Exercise 2.1: Explode orders to show one row per item

exercise_2_1 = orders_df.select(
    "order_id",
    "customer_id",
    "order_date",
    explode("items").alias("item")
).select(
    "order_id",
    "customer_id",
    "order_date",
    col("item.product_id"),
    col("item.product_name"),
    col("item.quantity"),
    col("item.unit_price")
)

print("Exercise 2.1 - Exploded order items:")
exercise_2_1.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.2: Calculate total revenue per product

exercise_2_2 = exercise_2_1.groupBy("product_id", "product_name") \
    .agg(
        sum(col("quantity") * col("unit_price")).alias("total_revenue"),
        sum("quantity").alias("total_quantity")
    ) \
    .orderBy(desc("total_revenue"))

print("Exercise 2.2 - Revenue per product:")
exercise_2_2.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.3: Find average number of items per order

exercise_2_3 = orders_df.select(
    avg(size("items")).alias("avg_items_per_order")
)

print("Exercise 2.3 - Average items per order:")
exercise_2_3.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 3: Working with Maps and JSON
# 
# Using the **products_df** DataFrame:
# 1. Extract the 'brand' from specifications for all products
# 2. Add a new key-value pair to the specifications map
# 3. Convert the entire products DataFrame to JSON and back

# CELL ********************

# Exercise 3.1: Extract brand from specifications

exercise_3_1 = products_df.select(
    "product_id",
    "product_name",
    col("specifications")["brand"].alias("brand")
)

print("Exercise 3.1 - Brands extracted:")
exercise_3_1.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 3.2: Add new key-value pair to specifications

exercise_3_2 = products_df.select(
    "product_id",
    "product_name",
    map_concat(
        col("specifications"),
        create_map(lit("in_stock"), lit("yes")) #example
    ).alias("specifications_updated")
)

print("Exercise 3.2 - Specifications with new key:")
exercise_3_2.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 3.3: Convert to JSON and back

# Convert to JSON
products_as_json = products_df.select(
    to_json(struct("*")).alias("json_data")
)

print("Products as JSON strings:")
products_as_json.show(truncate=False)

# Convert back from JSON
products_schema = products_df.schema

products_from_json = products_as_json.select(
    from_json("json_data", products_schema).alias("data")
).select("data.*")

print("Products restored from JSON:")
products_from_json.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Key Takeaways
# 
# Congratulations! You've completed the Complex Types lesson. Let's summarize what we've covered:
# 
# ### **Complex Types Mastered**:
# - **Arrays**: Ordered collections of same-type elements
# - **Maps**: Key-value pair collections for flexible attributes
# - **Structs**: Named fields with different types (like objects)
# 
# ### **Key Functions Learned**:
# - **explode() / explode_outer()**: Flatten arrays and maps into rows
# - **posexplode()**: Flatten with position index
# - **transform() / filter() / aggregate()**: Higher-order array functions
# - **from_json() / to_json()**: Parse and create JSON
# - **struct() / array() / create_map()**: Create complex types from columns
# 
# ### **Best Practices**:
# 1. **Use Parquet or Delta** for storing complex types
# 2. **Provide explicit schemas** when reading JSON
# 3. **Use higher-order functions** when you don't need to explode
# 4. **Access nested fields** with dot notation or getItem()
# 5. **Use explode_outer** when you need to preserve NULL/empty arrays

