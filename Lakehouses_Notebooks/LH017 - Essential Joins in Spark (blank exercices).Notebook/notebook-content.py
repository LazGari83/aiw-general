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

# # LH017 🟢 Essential Joins in Spark
# ## Fundamentals Module - Lesson 1
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the first lesson in our Fundamentals module! In this comprehensive lesson, we'll explore the essential joins in Apache Spark, covering when and how to use each type effectively.
# 
# ### What you'll learn in this lesson:
# - **Inner, Left, Right, Full, Semi, and Anti Joins** - Understanding each join type and their use cases
# - **When to use each join type** - Best practices for choosing the right join strategy
# - **Join conditions** - Equal and non-equal join conditions
# - **Handling nulls in joins** - Understanding how nulls behave in different join operations
# - **Join order and performance impact** - How join order affects query plans and performance
# - **Common errors and how to avoid them** - Troubleshooting typical join problems
# 
# ### Prerequisites:
# - Basic understanding of DataFrames in Spark
# - Familiarity with SQL concepts
# - Understanding of relational data concepts


# MARKDOWN ********************

# ## Setting Up Our Environment
# 
# Let's start by importing the necessary libraries and creating some sample datasets that we'll use throughout this lesson to demonstrate different join operations.

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

# ### Creating Sample Datasets
# 
# For our joins demonstration, we'll create realistic datasets representing:
# - **Customers**: Customer information with customer_id, name, and email
# - **Orders**: Order information with order_id, customer_id, and order_amount
# - **Products**: Product information with product_id, product_name, and category
# - **Order_Items**: Line items connecting orders to products
# 
# These datasets will help us understand real-world join scenarios.

# CELL ********************

# Create Customers DataFrame
customers_data = [
    (1, "Alice Johnson", "alice@email.com", "New York"),
    (2, "Bob Smith", "bob@email.com", "Los Angeles"),
    (3, "Carol Davis", "carol@email.com", "Chicago"),
    (4, "David Wilson", "david@email.com", "Houston"),
    (5, "Emma Brown", "emma@email.com", "Phoenix"),
    (6, "Frank Miller", "frank@email.com", "Philadelphia")
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

print("Customers DataFrame:")
customers_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Orders DataFrame
orders_data = [
    (101, 1, "2024-01-15", 150.00, "completed"),
    (102, 2, "2024-01-16", 89.50, "completed"),
    (103, 1, "2024-01-18", 245.75, "pending"),
    (104, 3, "2024-01-20", 67.25, "completed"),
    (105, 4, "2024-01-22", 312.00, "completed"),
    (106, 2, "2024-01-25", 123.45, "cancelled"),
    (107, 7, "2024-01-26", 78.90, "completed"),  # customer_id 7 doesn't exist in customers
    (108, None, "2024-01-27", 99.99, "pending")   # NULL customer_id
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

orders_df = spark.createDataFrame(orders_data, orders_schema)

print("Orders DataFrame:")
orders_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Order Items DataFrame

order_items_data = [
    (1, "2024-01-15", 1001, 1, 999.99),  # Alice, Laptop
    (1, "2024-01-18", 1002, 2, 59.98),   # Alice, 2x Wireless Mouse
    (2, "2024-01-16", 1004, 1, 79.99),   # Bob, Coffee Maker
    (3, "2024-01-20", 1003, 1, 199.99),  # Carol, Office Chair
    (4, "2024-01-22", 1005, 2, 99.98),   # David, 2x Desk Lamp
    (4, "2024-01-22", 1006, 1, 699.99)   # David, Smartphone (same day)
]

order_items_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("line_total", DoubleType(), True)
])

order_items_df = spark.createDataFrame(order_items_data, order_items_schema)

print("Order Items DataFrame:")
order_items_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Products DataFrame
products_data = [
    (1001, "Laptop Pro", "Electronics", 999.99),
    (1002, "Wireless Mouse", "Electronics", 29.99),
    (1003, "Office Chair", "Furniture", 199.99),
    (1004, "Coffee Maker", "Appliances", 79.99),
    (1005, "Desk Lamp", "Furniture", 49.99),
    (1006, "Smartphone", "Electronics", 699.99)
]

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True)
])

products_df = spark.createDataFrame(products_data, products_schema)

print("Products DataFrame:")
products_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Understanding Join Types
# 
# Before diving into code, let's understand what each join type does and when to use them:
# 
# ### Join Types Overview:
# - **Inner Join**: Returns only matching records from both DataFrames
# - **Left Join (Left Outer)**: Returns all records from left DF + matching records from right DF
# - **Right Join (Right Outer)**: Returns all records from right DF + matching records from left DF
# - **Full Join (Full Outer)**: Returns all records from both DataFrames
# - **Semi Join**: Returns records from left DF that have matches in right DF (like EXISTS)
# - **Anti Join**: Returns records from left DF that have NO matches in right DF (like NOT EXISTS)
# 
# Let's explore each type with practical examples!

# MARKDOWN ********************

# ### 1.1 Inner Join
# 
# **Inner joins** return only the records that have matching values in both DataFrames. This is the most restrictive join type and is ideal when you only want data that exists in both datasets.
# 
# **Use case**: "Show me customers who have placed orders"

# MARKDOWN ********************

# - most restrcted join type 

# CELL ********************

# Inner Join: Customers with Orders
inner_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "inner"
)

print("Inner Join - Customers with Orders:")
inner_join_result.select(
    customers_df.customer_id,
    "customer_name", 
    "email", 
    "order_id", 
    "order_amount", 
    "status"
).show()

print(f"Total records: {inner_join_result.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:  
# - Customer 5 (Emma) and Customer 6 (Frank) don't appear because they have no orders
# - Order 107 (customer_id 7) doesn't appear because customer 7 doesn't exist
# - Order 108 (NULL customer_id) doesn't appear because NULL doesn't match any customer_id

# MARKDOWN ********************

# ### 1.2 Left Join (Left Outer Join)
# 
# **Left joins** return ALL records from the left DataFrame and matching records from the right DataFrame. If no match exists, NULL values are returned for the right DataFrame columns.
# 
# **Use case**: "Show me all customers, and their orders if they have any"

# CELL ********************

# Left Join: All Customers with their Orders (if any)
left_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "left"
)

print("Left Join - All Customers with Orders:")
left_join_result.select(
    customers_df.customer_id,
    "customer_name", 
    "email", 
    "order_id", 
    "order_amount", 
    "status"
).orderBy(customers_df.customer_id).show()

print(f"Total records: {left_join_result.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:
# - ALL customers appear in the result
# - Emma and Frank have NULL values for order columns because they have no orders
# - Order 107 still doesn't appear because it references a non-existent customer

# MARKDOWN ********************

# ### 1.3 Right Join (Right Outer Join)
# 
# **Right joins** return ALL records from the right DataFrame and matching records from the left DataFrame. This is less commonly used but useful in specific scenarios.
# 
# **Use case**: "Show me all orders, and customer information if it exists"

# CELL ********************

# Right Join: All Orders with Customer information (if any)
right_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "right"
)

print("Right Join - All Orders with Customer info (if exists):")
right_join_result.select(
    "customer_name", 
    "email", 
    orders_df.customer_id,
    "order_id", 
    "order_amount", 
    "status"
).orderBy("order_id").show()

print(f"Total records: {right_join_result.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:
# - ALL orders appear in the result
# - Order 107 (customer_id 7) has NULL customer information because customer 7 doesn't exist
# - Order 108 (NULL customer_id) also has NULL customer information
# - Customers without orders (Emma, Frank) don't appear

# MARKDOWN ********************

# ### 1.4 Full Join (Full Outer Join)
# 
# **Full joins** return ALL records from both DataFrames. When there's no match, NULL values are returned for the missing side.
# 
# **Use case**: "Show me all customers and all orders, whether they match or not"

# CELL ********************

# Full Join: All Customers and All Orders
full_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "full"
)

print("Full Join - All Customers and All Orders:")
full_join_result.select(
    customers_df.customer_id.alias("cust_id"),
    "customer_name", 
    orders_df.customer_id.alias("order_cust_id"),
    "order_id", 
    "order_amount", 
    "status"
).orderBy(coalesce(customers_df.customer_id, orders_df.customer_id)).show()

print(f"Total records: {full_join_result.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:
# - ALL customers appear (including Emma and Frank with NULL order information)
# - ALL orders appear (including orphaned orders with NULL customer information)
# - This gives us the complete picture of both datasets

# MARKDOWN ********************

# ### 1.5 Semi Join
# 
# **Semi joins** return records from the left DataFrame where there's a match in the right DataFrame, but they DON'T include columns from the right DataFrame. Think of it as a filtered version of the left DataFrame.
# 
# **Use case**: "Show me customers who have placed orders (but I don't need order details)"

# CELL ********************

# Semi Join: Customers who have orders (no order details)
semi_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "semi"
)

print("Semi Join - Customers who have placed orders:")
semi_join_result.show()

print(f"Total records: {semi_join_result.count()}")

# Compare with distinct customers from inner join
print("\nFor comparison - Distinct customers from inner join:")
inner_join_result.select(
    customers_df.customer_id,
    "customer_name", 
    "email", 
    "city"
).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:
# - Only customers with orders appear (no Emma or Frank)
# - No order columns are included in the result
# - Each customer appears only once (even if they have multiple orders)
# - This is more efficient than doing INNER JOIN

# MARKDOWN ********************

# ### 1.6 Anti Join
# 
# **Anti joins** return records from the left DataFrame where there's NO match in the right DataFrame. This is the opposite of a semi join.
# 
# **Use case**: "Show me customers who have NOT placed any orders"

# CELL ********************

# Anti Join: Customers who have NOT placed orders
anti_join_result = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "anti"
)

print("Anti Join - Customers who have NOT placed orders:")
anti_join_result.show()

print(f"Total records: {anti_join_result.count()}")

# Verify by checking left join where order_id is NULL
print("\nFor comparison - Left join filtered for NULL order_id:")
left_join_result.filter(col("order_id").isNull()).select(
    customers_df.customer_id,
    "customer_name", 
    "email", 
    "city"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Notice that**:
# - Only Emma and Frank appear (customers without orders)
# - This is more efficient than using LEFT JOIN with IS NULL filter
# - Perfect for finding "missing" relationships between datasets

# MARKDOWN ********************

# ## Part 2: Join Conditions - Equal vs Non-Equal
# 
# So far, we've used equality conditions (`==`) for our joins. However, Spark also supports non-equal join conditions, which can be useful for range queries, time-based joins, and more complex relationships.

# MARKDOWN ********************

# ### 2.1 Equal Join Conditions
# 
# These are the most common and efficient join conditions. Spark can optimize these well using techniques like broadcast joins and bucketing.

# CELL ********************

# Equal join conditions - different ways to write them

# Method 1: Using column expressions
result1 = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id, 
    "inner"
)

# Method 2: Using column names (when they're the same)
result2 = customers_df.join(
    orders_df, 
    "customer_id", 
    "inner"
)


# Method 3: Using arrays for multiple join keys
result3 = order_items_df.join(
    orders_df, 
    ["customer_id", "order_date"],  # Both columns exist in both DFs
    "inner"
)

print("Equal join conditions work the same way:")
print(f"Method 1 count: {result1.count()}")
print(f"Method 2 count: {result2.count()}")
print(f"Method 3 count: {result3.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2.2 Non-Equal Join Conditions
# 
# Non-equal joins are useful for range queries, finding relationships based on thresholds or time-based analysis. However, they're less efficient and should be used carefully.

# CELL ********************

# Let's create a customer segments DataFrame for demonstration
segments_data = [
    ("Bronze", 0.0, 100.0),
    ("Silver", 100.01, 250.0),
    ("Gold", 250.01, 500.0),
    ("Platinum", 500.01, 999999.0)
]

segments_schema = StructType([
    StructField("segment", StringType(), True),
    StructField("min_amount", DoubleType(), True),
    StructField("max_amount", DoubleType(), True)
])

segments_df = spark.createDataFrame(segments_data, segments_schema)

print("Customer Segments:")
segments_df.show()

# Non-equal join: Match orders to segments based on amount ranges
range_join_result = orders_df.join(
    segments_df,
    (orders_df.order_amount >= segments_df.min_amount) & 
    (orders_df.order_amount <= segments_df.max_amount),
    "inner"
)

print("\nOrders with Customer Segments (Range Join):")
range_join_result.select(
    "order_id",
    "order_amount",
    "segment",
    "min_amount",
    "max_amount"
).orderBy("order_amount").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 3: Handling Nulls in Joins
# 
# Understanding how NULLs behave in joins is crucial for data integrity and getting expected results. Let's explore different scenarios with NULL values.

# CELL ********************

# Let's examine our data to see NULL values
print("Orders with potential NULL customer_id:")
orders_df.filter(col("customer_id").isNull()).show()

print("\nAll orders showing customer_id values:")
orders_df.select("order_id", "customer_id", "order_amount").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3.1 NULL Behavior in Different Join Types

# CELL ********************

# Create a summary of how NULLs behave in each join type
join_types = ["inner", "left", "right", "full"]

for join_type in join_types:
    result = customers_df.join(
        orders_df, 
        customers_df.customer_id == orders_df.customer_id, 
        join_type
    )
    
    count = result.count()
    null_customer_orders = result.filter(col("customer_name").isNull()).count()
    null_order_customers = result.filter(col("order_id").isNull()).count()

    
    
    print(f"{join_type.upper()} JOIN:")
    print(f"  Total records: {count}")
    result.show()
    print(f"  Orders with NULL customer info: {null_customer_orders}")
    print(f"  Customers with NULL order info: {null_order_customers}")
    print()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3.2 Handling NULLs in Join Conditions
# 
# Sometimes you may want to include NULL values in your joins. Spark provides several ways to handle this.

# CELL ********************

# Let's create a more realistic scenario with NULL values in both DataFrames
# Add some customers with NULL customer_id (data quality issues)
customers_with_nulls_data = [
    (1, "Alice Johnson", "alice@email.com", "New York"),
    (2, "Bob Smith", "bob@email.com", "Los Angeles"),
    (None, "Unknown Customer 1", "unknown1@email.com", "Unknown")
]

customers_with_nulls = spark.createDataFrame(customers_with_nulls_data, customers_schema)

print("Customers with NULL customer_id:")
customers_with_nulls.show()

print("\nOrders with NULL customer_id (from our original data):")
orders_df.filter(col("customer_id").isNull()).show()

print("STANDARD JOIN BEHAVIOR WITH NULLS")

standard_join = customers_with_nulls.join(
    orders_df,
    "customer_id",
    "inner"
)
print(f"Standard inner join: {standard_join.count()} records")
print("Notice: NULL customer_id values are excluded (NULL != NULL in joins)")
standard_join.show()


print("Method 1: Null Safe Join - Treat Null as mnatching")

# Method 1: Explicit NULL handling
null_safe_join = customers_with_nulls.join(
    orders_df,
    (
        # Regular equality condition
        (customers_with_nulls.customer_id == orders_df.customer_id) |
        # PLUS: Match when both are NULL
        (customers_with_nulls.customer_id.isNull() & orders_df.customer_id.isNull())
    ),
    "inner"
)
print(f"Null-safe join: {null_safe_join.count()} records")
print("Notice: Now NULL customer_id values are matched together!")
null_safe_join.select(
    customers_with_nulls.customer_id.alias("cust_id"),
    "customer_name",
    orders_df.customer_id.alias("order_cust_id"),
    "order_id",
    "order_amount"
).show()

print("\n" + "="*60)
print("Method 2: Replace Nulls before joining")

# Method 2: Replace NULLs with a default value (more common approach)
customers_clean = customers_with_nulls.fillna({"customer_id": -1})
orders_clean = orders_df.fillna({"customer_id": -1})

clean_join = customers_clean.join(orders_clean, "customer_id", "inner")
print(f"Join after fillna(): {clean_join.count()} records")
print("All NULL customer_id values are now -1 and can be joined:")
clean_join.filter(col("customer_id") == -1).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 4: Join Order and Performance Impact
# 
# The order in which you join DataFrames can significantly impact performance. Spark's Catalyst optimizer does its best to optimize joins, but understanding join strategies helps you write better queries.

# MARKDOWN ********************

# ### 4.1 Understanding Join Strategies
# 
# Spark uses different strategies for joins based on data size and join conditions:
# 
# - **Broadcast Hash Join**: Small table is broadcast to all nodes (most efficient for small tables)
# - **Sort Merge Join**: Both tables are sorted and merged (good for large tables with sorted data)
# - **Shuffle Hash Join**: Data is shuffled across nodes based on join keys
# - **Cartesian Join**: Every row from one table is joined with every row from another (avoid this!)

# CELL ********************

# Let's examine the query plan

# Large table first (orders) - may require shuffling
plan2 = orders_df.join(customers_df, "customer_id", "inner")
print("Larger table (orders) joined with small table (customers)")
plan2.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4.2 Join Optimization Tips
# 
# Here are practical tips for optimizing joins:

# CELL ********************

# Tip 1: Use broadcast hints for small tables

# Force broadcast of small table
broadcast_join = orders_df.join(broadcast(customers_df), "customer_id", "inner")
print("\nForced broadcast join plan:")
broadcast_join.explain()

print("\n" + "="*40 + "\n")

# Compare with regular join
regular_join = orders_df.join(customers_df, "customer_id", "inner")
print("Regular join plan (no broadcast hint):")
regular_join.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tip 2: Filter before joining to reduce data size
filtered_customers = customers_df.filter(col("city").isin(["New York", "Los Angeles"]))
filtered_orders = orders_df.filter(col("status") == "completed")

optimized_join = filtered_customers.join(filtered_orders, "customer_id", "inner")
print("Join with pre-filtering:")
optimized_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Tip 3: Use column pruning - select only needed columns
efficient_join = customers_df.select("customer_id", "customer_name") \
    .join(orders_df.select("customer_id", "order_id", "order_amount"), "customer_id", "inner")

print("\nJoin with column pruning:")
print(f"Columns in result: {efficient_join.columns}")
efficient_join.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 5: Common Errors and How to Avoid Them
# 
# Let's explore common join-related errors and how to prevent or fix them.

# MARKDOWN ********************

# ### 5.1 Ambiguous Column References

# CELL ********************

# Problem: Ambiguous column references after join
joined_df = customers_df.join(orders_df, customers_df.customer_id == orders_df.customer_id, "inner")

print("Joined DataFrame columns:")
print(joined_df.columns)

# This will cause an error - which customer_id?
try:
    joined_df.select("customer_id").show()
except Exception as e:
    print(f"\nError: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Solutions:
print("\nSolution 1: Use DataFrame aliases to specify columns")
joined_df.select(customers_df.customer_id.alias("cust_id"), "customer_name", "order_id").show()

print("\nSolution 2: Use different join syntax to avoid duplicate column names")
clean_join = customers_df.join(orders_df, "customer_id", "inner")
print(f"Clean join columns: {clean_join.columns}")
clean_join.select("customer_id", "customer_name", "order_id").show()

print("\nSolution 3: Rename columns before joining")
orders_renamed = orders_df.withColumnRenamed("customer_id", "order_customer_id")
renamed_join = customers_df.join(orders_renamed, customers_df.customer_id == orders_renamed.order_customer_id, "inner")
print(f"Renamed join columns: {renamed_join.columns}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 5.2 Cartesian Products and Performance Issues

# CELL ********************

# Problem: Accidental cartesian joins
print("Customer count:", customers_df.count())
print("Orders count:", orders_df.count())

# This creates a cartesian product - every customer with every order!
# DON'T do this in production with large datasets
cartesian_result = customers_df.crossJoin(orders_df)
cartesian_count = cartesian_result.count()
print(f"\nCartesian product count: {cartesian_count} (6 customers × 8 orders = 48 rows)")

cartesian_result.select("customer_name", "order_id", "order_amount").show(10)

# How to avoid: Always use proper join conditions
proper_join = customers_df.join(orders_df, "customer_id", "inner")
print(f"\nProper join count: {proper_join.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## When to Use Cross Joins (and When NOT to!)
# 
# ### **GOOD Use Cases - Cross Joins Make Sense:**
# 
# **Small Reference Tables (< 1000 rows)**
# - **Example**: 4 sizes × 6 colors × 2 materials = 48 product variants
# - **Why it works**: Small result set, intentional combinations
# - **Risk**: Low - manageable data size
# 
# **Generate All Possible Combinations**
# - **Example**: Testing all user types against all features
# - **Purpose**: You WANT every possible pairing
# - **Use case**: A/B testing matrices, configuration validation
# 
# **Fill Missing Data Points**
# - **Example**: Ensure every date has an entry for every metric (even if zero)
# - **Result**: Complete time series without gaps
# - **Business need**: Reports must show all periods, even with no activity
# 
# **Zero-Fill Reports**
# - **Example**: Sales report showing ALL regions × ALL products (even 0€ sales)
# - **Why**: Business wants to see "we sold nothing in Region X"
# - **Alternative**: Would require complex LEFT JOIN logic
# 
# ### **BAD Use Cases - Avoid Cross Joins:**
# 
# **Large Tables (1M+ rows)**
# - **Example**: 1M customers × 10M orders = 10 trillion rows!
# - **Consequence**: Cluster crash, out of memory errors...
# - **Check**: If both tables are big, don't cross join!
# 
# **Accidental Cartesian Products**
# - **Example**: Forgot the `ON customer_id = order.customer_id` condition
# - **Result**: Unintended explosion of data
# - **How to avoid**: Always double-check your join conditions
# 
# **When You Need Real Relationships**
# - **Example**: Show customers with their actual orders
# - **Wrong**: Cross join gives every customer every order
# - **Right**: Use INNER/LEFT/RIGHT joins with proper conditions
# 
# If you're asking "Do I need a cross join?" - you probably don't. Cross joins are for specific scenarios where you deliberately want ALL combinations, not for matching related data.
# 
# ### **Quick Decision Tree:**
# 1. **Do you want ALL combinations?** → Cross Join
# 2. **Do you want matching records?** → Regular Join  
# 3. **Are both tables large?** → Think twice!
# 4. **Did you forget a join condition?** → Fix it!


# MARKDOWN ********************

# ### 5.3 Data Skew and Uneven Partitioning
# 
# **Data skew** occurs when data is unevenly distributed across partitions, causing some tasks to process much more data than others. This creates performance bottlenecks where a few "hot" partitions slow down the entire job.
# 
# #### What causes data skew?
# 
# **Uneven key distribution**: Some join keys appear much more frequently than others.
# 
# #### Why is this a problem in Spark joins?
# ```
# Normal distribution:    Skewed distribution:
# Partition 1: 1000 rows  Partition 1: 50,000 rows  ← BOTTLENECK!
# Partition 2: 1200 rows  Partition 2: 100 rows     ← Idle
# Partition 3: 900 rows   Partition 3: 150 rows     ← Idle
# Partition 4: 1100 rows  Partition 4: 200 rows     ← Idle
# ```
# 
# One partition takes 10x longer, while others finish quickly and sit idle.
# 
# #### Common symptoms:
# - **Long-running stages** with most tasks completing quickly
# - **Out of memory errors** on specific executors
# - **Straggler tasks** that hold up the entire job
# - **Uneven resource utilization** across the cluster
# 
# #### Quick solutions (covered in detail later):
# - **Salting**: Add random values to break up hot keys
# - **Bucketing**: Pre-partition data by join keys
# - **Broadcast joins**: Send small tables to all nodes
# - **Repartitioning**: Redistribute data before joining
# 
# We will explore these techniques in depth in future lessons on performance optimization.


# MARKDOWN ********************

# ## Part 6: Using SQL for Joins
# 
# Everything we've done with DataFrame API can also be accomplished using Spark SQL. Let's see how to perform the same operations using SQL syntax.

# CELL ********************

# Create temporary views for SQL access
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")
products_df.createOrReplaceTempView("products")
segments_df.createOrReplaceTempView("segments")

print("Temporary views created successfully!")
test_query = spark.sql("SELECT COUNT(*) as customer_count FROM customers")
print(f"Test query result: {test_query.collect()[0]['customer_count']} customers in view")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SQL examples of all join types

# Inner Join
sql_inner = spark.sql("""
    SELECT c.customer_id, c.customer_name, c.email, 
           o.order_id, o.order_amount, o.status
    FROM customers c
    INNER JOIN orders o ON c.customer_id = o.customer_id
    ORDER BY c.customer_id, o.order_id
""")

print("SQL Inner Join:")
sql_inner.show()

# Left Join
sql_left = spark.sql("""
    SELECT c.customer_id, c.customer_name, 
           o.order_id, o.order_amount
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    ORDER BY c.customer_id
""")

print("\nSQL Left Join:")
sql_left.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# SQL Semi and Anti Joins

# Semi Join using EXISTS
sql_semi = spark.sql("""
    SELECT customer_id, customer_name, email
    FROM customers c
    WHERE EXISTS (
        SELECT 1 FROM orders o 
        WHERE o.customer_id = c.customer_id
    )
""")

print("SQL Semi Join (using EXISTS):")
sql_semi.show()

# Anti Join using NOT EXISTS
sql_anti = spark.sql("""
    SELECT customer_id, customer_name, email
    FROM customers c
    WHERE NOT EXISTS (
        SELECT 1 FROM orders o 
        WHERE o.customer_id = c.customer_id
    )
""")

print("\nSQL Anti Join (using NOT EXISTS):")
sql_anti.show()

# Range Join using SQL
sql_range = spark.sql("""
    SELECT o.order_id, o.order_amount, s.segment
    FROM orders o
    INNER JOIN segments s ON o.order_amount BETWEEN s.min_amount AND s.max_amount
    ORDER BY o.order_amount
""")

print("\nSQL Range Join:")
sql_range.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Practice Exercises
# 
# Now it's your turn! Complete these exercises to test your understanding of joins in Spark.

# MARKDOWN ********************

# ### Exercise 1: Customer Order Analysis
# 
# Using the datasets we've created, write queries to answer these questions:
# 
# 1. **Find all customers from New York who have placed orders**
# 2. **Calculate the total order amount for each customer**
# 3. **Find customers who have placed orders worth more than $200**

# CELL ********************

# Exercise 1.1: Find all customers from New York who have placed orders
# Your code here:
ex1_1 = customers_df.filter(col("City")== "New York")\
.join(orders_df,"customer_id","inner")\
.select("customer_name", "email", "order_id", "order_amount")

print("Exercise 1.1 - NY customers with orders:")
exercise_1_1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.2: Calculate total order amount for each customer
# Your code here:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.3: Find customers who have placed orders worth more than $200
# Your code here:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 2: Advanced Join Scenarios
# 
# 1. **Find customers who have never placed an order**
# 2. **Find all orders that don't have corresponding customers**
# 3. **Create a comprehensive customer report showing all customers with their order statistics (including those with no orders)**

# CELL ********************

# Exercise 2.1: Find customers who have never placed an order
# Your code here:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.2: Find all orders that don't have corresponding customers
# Your code here:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.3: customer report
# Your code here:


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Key Takeaways
# 
# Congratulations! You've completed the Essential Joins in Spark lesson. Let's summarize what we've covered:
# 
# ### **Join Types Mastered**:
# - **Inner Join**: Returns only matching records from both sides
# - **Left Join**: All records from left + matching from right
# - **Right Join**: All records from right + matching from left
# - **Full Join**: All records from both sides
# - **Semi Join**: Left records that have matches (no right columns)
# - **Anti Join**: Left records that have NO matches
# 
# ### **Key Concepts**:
# - **Equal vs Non-Equal conditions**: Use equal conditions for better performance
# - **NULL handling**: NULLs don't match in standard joins
# - **Performance optimization**: Use filters, column pruning, and appropriate join strategies
# 
# ### **Common Pitfalls to Avoid**:
# - Ambiguous column references after joins
# - Accidental cartesian products
# - Memory issues with large joins
# 
# ### **Best Practices**:
# 1. **Choose the right join type** for your use case
# 2. **Filter early** to reduce data size before joining
# 3. **Use column aliases** to avoid ambiguity
# 4. **Monitor query plans** to ensure optimal performance
# 5. **Use broadcast joins** for small tables
# 6. **Handle NULLs explicitly** when needed
# 
# You're now equipped with essential join knowledge to tackle complex data engineering challenges in Spark!


# MARKDOWN ********************

# ### Cleanup

# CELL ********************

# Clean up temporary views
spark.sql("DROP VIEW IF EXISTS customers")
spark.sql("DROP VIEW IF EXISTS orders")
spark.sql("DROP VIEW IF EXISTS products")
spark.sql("DROP VIEW IF EXISTS segments")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
