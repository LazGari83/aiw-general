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

# # LH018 🟢 Window Functions in Spark
# ## Fundamentals Module - Lesson 2
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the second lesson in our Fundamentals module! In this comprehensive lesson, we'll explore Window Functions in Apache Spark, one of the most powerful features for advanced data analysis and transformation.
# 
# ### What you'll learn in this lesson:
# - **Window Function Basics** - Understanding partitionBy and orderBy clauses
# - **Ranking Functions** - row_number, rank, and dense_rank with practical examples
# - **Lag and Lead Functions** - Accessing previous and next row values
# - **Aggregate Window Functions** - Moving averages, running totals, and cumulative sums
# - **Time-based Windows** - Working with time series data and time windows
# - **Data Quality Use Cases** - Deduplication and finding last/first values
# - **Performance Optimization** - Understanding costs and optimization strategies
# 
# ### Prerequisites:
# - Basic understanding of DataFrames and SQL
# - Familiarity with aggregate functions
# - Understanding of partitioning concepts


# MARKDOWN ********************

# ## What are Window Functions?
# 
# Window functions perform calculations across a **set of table rows** that are somehow related to the current row. Unlike aggregate functions that return one result per group, window functions return a value for **each row** in the result set.
# 
# ![image-alt-text](https://api.datalemur.com/assets/f7b35d5a-07d0-4d4d-88a2-9f31d0b5e7f7)
# 
# ### Key Concepts:
# - **Window**: A set of rows defined by the `PARTITION BY` and `ORDER BY` clauses
# - **Partition**: Groups of rows that share the same values for partitioning columns
# - **Ordering**: How rows within each partition are sorted
# - **Frame**: A subset of the partition (optional - defines row range for calculation)
# 
# ### Common Use Cases:
# - Ranking records within groups
# - Calculating running totals and moving averages
# - Finding previous/next values in time series
# - Data deduplication
# - Percentile calculations

# MARKDOWN ********************

# ## Setting Up Our Environment
# 
# Let's import the necessary libraries and create realistic sample datasets to demonstrate window functions effectively.

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import datetime

print("Libraries imported successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating Sample Datasets
# 
# We'll create several datasets to demonstrate different window function scenarios:
# - **Sales**: Daily sales data with multiple salespeople and regions
# - **Employee Performance**: Monthly performance metrics
# - **Stock Prices**: Time series data for stock analysis
# - **Web Analytics**: User session data for behavioral analysis
# 
# These datasets will help us understand real-world window function applications.

# CELL ********************

# Create Sales DataFrame
sales_data = [
    ("2024-01-01", "Alice", "North", 1500.00, 3),
    ("2024-01-01", "Bob", "South", 1200.00, 2),
    ("2024-01-01", "Carol", "North", 1800.00, 4),
    ("2024-01-02", "Alice", "North", 1750.00, 4),
    ("2024-01-02", "Bob", "South", 1400.00, 3),
    ("2024-01-02", "Carol", "North", 1600.00, 3),
    ("2024-01-03", "Alice", "North", 2000.00, 5),
    ("2024-01-03", "Bob", "South", 1100.00, 2),
    ("2024-01-03", "Carol", "North", 1900.00, 4),
    ("2024-01-04", "Alice", "North", 1650.00, 3),
    ("2024-01-04", "Bob", "South", 1550.00, 4),
    ("2024-01-04", "Carol", "North", 1700.00, 3),
    ("2024-01-05", "Alice", "North", 1800.00, 4),
    ("2024-01-05", "Bob", "South", 1300.00, 3),
    ("2024-01-05", "Carol", "North", 2100.00, 5)
]

sales_schema = StructType([
    StructField("sale_date", StringType(), True),
    StructField("salesperson", StringType(), True),
    StructField("region", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("units_sold", IntegerType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema) \
    .withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

print("Sales DataFrame:")
sales_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Employee Performance DataFrame
performance_data = [
    ("Alice", "2024-01", 95, 8500.00, "A"),
    ("Alice", "2024-02", 88, 7800.00, "B"),
    ("Alice", "2024-03", 92, 8200.00, "A"),
    ("Bob", "2024-01", 78, 6500.00, "C"),
    ("Bob", "2024-02", 85, 7200.00, "B"),
    ("Bob", "2024-03", 90, 8000.00, "A"),
    ("Carol", "2024-01", 92, 8300.00, "A"),
    ("Carol", "2024-02", 89, 7900.00, "B"),
    ("Carol", "2024-03", 94, 8600.00, "A"),
    ("David", "2024-01", 82, 7000.00, "B"),
    ("David", "2024-02", 79, 6800.00, "C"),
    ("David", "2024-03", 87, 7500.00, "B"),
    ("Louis", "2024-01", 95, 7000.00, "A"),
    ("Louis", "2024-02", 72, 6800.00, "C"),
    ("Louis", "2024-03", 88, 7500.00, "B")
]

performance_schema = StructType([
    StructField("employee", StringType(), True),
    StructField("month", StringType(), True),
    StructField("performance_score", IntegerType(), True),
    StructField("revenue_generated", DoubleType(), True),
    StructField("grade", StringType(), True)
])

performance_df = spark.createDataFrame(performance_data, performance_schema)

print("Employee Performance DataFrame:")
performance_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Stock Prices DataFrame (with duplicates for deduplication examples)
stock_data = [
    ("AAPL", "2024-01-01", 150.25, 148.50, 152.10, 149.80, 1000000),
    ("AAPL", "2024-01-02", 149.80, 147.20, 151.50, 150.45, 1200000),
    ("AAPL", "2024-01-03", 150.45, 149.10, 153.00, 152.30, 950000),
    ("AAPL", "2024-01-04", 152.30, 150.75, 154.20, 153.85, 1100000),
    ("AAPL", "2024-01-05", 153.85, 152.40, 155.60, 154.20, 1300000),
    # Duplicate entry for deduplication example
    ("AAPL", "2024-01-05", 153.85, 152.40, 155.60, 154.20, 1300000),
    ("MSFT", "2024-01-01", 380.50, 378.20, 382.75, 381.15, 800000),
    ("MSFT", "2024-01-02", 381.15, 379.80, 384.50, 383.20, 750000),
    ("MSFT", "2024-01-03", 383.20, 381.40, 385.90, 384.75, 900000),
    ("MSFT", "2024-01-04", 384.75, 382.60, 387.20, 386.45, 850000),
    ("MSFT", "2024-01-05", 386.45, 384.80, 388.90, 387.60, 920000)
]

stock_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("date", StringType(), True),
    StructField("open_price", DoubleType(), True),
    StructField("low_price", DoubleType(), True),
    StructField("high_price", DoubleType(), True),
    StructField("close_price", DoubleType(), True),
    StructField("volume", IntegerType(), True)
])

stock_df = spark.createDataFrame(stock_data, stock_schema) \
    .withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

print("Stock Prices DataFrame:")
stock_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Window Function Basics: partitionBy and orderBy
# 
# The foundation of window functions lies in understanding how to define the **window specification**. This consists of:
# 
# ### Window Specification Components:
# 1. **PARTITION BY**: Divides the result set into partitions (similar to GROUP BY)
# 2. **ORDER BY**: Defines the logical order of rows within each partition
# 
# ### Basic Syntax:
# ```python
# from pyspark.sql.window import Window
# 
# # Define window specification
# window_spec = Window.partitionBy("column1").orderBy("column2")
# 
# # Apply window function
# df.withColumn("result", window_function().over(window_spec))
# ```

# CELL ********************

# Example 1: Basic window specification
# Let's rank salespeople by daily sales within each region

# Define window: partition by region and date, order by amount (descending)
window_by_region_date = Window.partitionBy("region", "sale_date").orderBy(desc("amount"))

# Apply row_number window function
sales_with_rank = sales_df.withColumn(
    "daily_rank_in_region", 
    row_number().over(window_by_region_date)
)

print("Sales with daily ranking by region:")
sales_with_rank.select("sale_date", "region", "salesperson", "amount", "daily_rank_in_region") \
    .orderBy("sale_date", "region", "daily_rank_in_region").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example 2: Different partitioning strategy
# Rank all sales by salesperson across all dates

window_by_salesperson = Window.partitionBy("salesperson").orderBy(desc("amount"))

sales_personal_rank = sales_df.withColumn(
    "personal_best_rank", 
    row_number().over(window_by_salesperson)
)

print("Each salesperson's best sales ranked:")
sales_personal_rank.select("salesperson", "sale_date", "amount", "personal_best_rank") \
    .orderBy("salesperson", "personal_best_rank").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ranking Functions: row_number, rank, and dense_rank
# 
# Ranking functions assign a rank to each row within a window partition. Understanding the differences is crucial:
# 
# ### Ranking Function Comparison:
# | Function | Behavior with Ties | Example |
# |----------|-------------------|----------|
# | **row_number()** | Always unique, arbitrary order for ties | 1, 2, 3, 4, 5 |
# | **rank()** | Gaps after ties | 1, 2, 2, 4, 5 |
# | **dense_rank()** | No gaps after ties | 1, 2, 2, 3, 4 |
# 
# ### When to Use Each:
# - **row_number**: When you need exactly one record per rank (deduplication)
# - **rank**: Traditional ranking with gaps (like Olympic medals)
# - **dense_rank**: Continuous ranking without gaps (like grade levels)

# CELL ********************

# Example: Comparing all ranking functions
# Let's rank performance scores within each month

window_by_month = Window.partitionBy("month").orderBy(desc("performance_score"))

ranking_comparison = performance_df.select(
    "month", "employee", "performance_score",
    row_number().over(window_by_month).alias("row_number"),
    rank().over(window_by_month).alias("rank"),
    dense_rank().over(window_by_month).alias("dense_rank")
)

print("Ranking Functions Comparison:")
ranking_comparison.orderBy("month", "row_number").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Practical Example: Top performer in each month
# Using row_number to get exactly one top performer per month

top_performers = performance_df.withColumn(
    "monthly_rank",
    row_number().over(window_by_month)
).filter(col("monthly_rank") == 1)

print("Top performer each month:")
top_performers.select("month", "employee", "performance_score", "revenue_generated").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Lag and Lead Functions
# 
# Lag and Lead functions provide access to **previous and next row values** within a window partition, making them essential for time series analysis and trend calculations.
# 
# ### Function Syntax:
# - **lag(column, offset, default)**: Gets value from `offset` rows before
# - **lead(column, offset, default)**: Gets value from `offset` rows after
# 
# ### Common Use Cases:
# - Calculating period-over-period changes
# - Finding trends and patterns
# - Identifying consecutive events
# - Creating running differences

# CELL ********************

# Example 1: Stock price analysis with lag and lead
# Calculate daily price changes and predict next day's opening

window_by_symbol = Window.partitionBy("symbol").orderBy("date")

stock_analysis = stock_df.select(
    "symbol", "date", "close_price",
    lag("close_price", 1).over(window_by_symbol).alias("prev_close"),
    lead("close_price", 1).over(window_by_symbol).alias("next_close"),
    (col("close_price") - lag("close_price", 1).over(window_by_symbol)).alias("daily_change"),
    round(
        ((col("close_price") - lag("close_price", 1).over(window_by_symbol)) / 
         lag("close_price", 1).over(window_by_symbol) * 100), 2
    ).alias("%_change")
)
#.filter(col("prev_close").isNotNull())  # Remove first row which has no previous value

print("Stock Price Analysis with Lag/Lead:")
stock_analysis.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example 2: Performance trend analysis
# Track month-over-month performance changes for each employee

window_by_employee = Window.partitionBy("employee").orderBy("month")

performance_trends = performance_df.select(
    "employee", "month", "performance_score", "revenue_generated",
    lag("performance_score", 1).over(window_by_employee).alias("prev_score"),
    (col("performance_score") - lag("performance_score", 1).over(window_by_employee)).alias("score_change"),
    lag("revenue_generated", 1).over(window_by_employee).alias("prev_revenue"),
    round(
        (col("revenue_generated") - lag("revenue_generated", 1).over(window_by_employee)), 2
    ).alias("revenue_change")
)

print("Employee Performance Trends:")
performance_trends.orderBy("employee", "month").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Moving Averages and Running Totals
# 
# Aggregate window functions allow you to perform calculations over a **frame of rows** rather than the entire partition. This is essential for moving averages, running totals, and cumulative calculations.
# 
# ### Frame Specifications:
# - **ROWS**: Physical number of rows
# - **RANGE**: Logical range based on values
# 
# ### Common Frame Patterns:
# - `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`: Running total
# - `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`: 3-period moving average
# - `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`: Reverse cumulative

# CELL ********************

# Example 1: Sales moving averages and running totals
# Calculate 3-day moving average and running total for each salesperson

window_salesperson_date = Window.partitionBy("salesperson").orderBy("sale_date")

# Different frame specifications
window_running_total = window_salesperson_date.rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_3day_ma = window_salesperson_date.rowsBetween(-2, Window.currentRow)

sales_analytics = sales_df.select(
    "salesperson", 
    "sale_date", 
    "amount",
    sum("amount").over(window_running_total).alias("running_total"),
    round(avg("amount").over(window_3day_ma), 2).alias("moving_avg_3d"),
    count("amount").over(window_running_total).alias("cumulative_days"),
    round(avg("amount").over(window_running_total), 2).alias("overall_avg")
)

print("Sales Analytics with Moving Averages:")
sales_analytics.orderBy("salesperson", "sale_date").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example 2: Stock price technical indicators
# Calculate moving averages for technical analysis

window_stock_date = Window.partitionBy("symbol").orderBy("date")
window_5day = window_stock_date.rowsBetween(-4, Window.currentRow)
window_3day = window_stock_date.rowsBetween(-2, Window.currentRow)

technical_analysis = stock_df.select(
    "symbol", 
    "date", 
    "close_price", 
    "volume",
    round(avg("close_price").over(window_3day), 2).alias("sma_3"),
    round(avg("close_price").over(window_5day), 2).alias("sma_5"),
    sum("volume").over(window_3day).alias("volume_3d"),
    round(stddev("close_price").over(window_5day), 2).alias("volatility_5d")
).filter(col("symbol") == "AAPL")

print("Technical Analysis for AAPL:")
technical_analysis.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Data Quality: Deduplication and Last Value
# 
# Window functions are extremely powerful for **data quality** tasks, especially deduplication and finding the most recent or relevant records.
# 
# ### Common Data Quality Patterns:
# - **Deduplication**: Use `row_number()` to identify and keep/remove duplicates
# - **Latest Record**: Use `last_value()` or `first_value()` to find most recent data
# - **Data Validation**: Use window functions to identify outliers and anomalies
# 
# ### Deduplication Strategy:
# 1. Identify partition columns (what makes a record unique)
# 2. Choose ordering criteria (what defines "latest" or "best")
# 3. Use `row_number() = 1` to keep one record per group

# CELL ********************

# Example 1: Deduplication
# Remove duplicate stock records (we have duplicate AAPL for 2024-01-05)

print("Original stock data with duplicates:")
stock_df.filter(col("symbol") == "AAPL").show()

# Define deduplication window: partition by symbol+date, order by volume (keep highest volume)
dedup_window = Window.partitionBy("symbol", "date").orderBy(desc("volume"))

# Deduplicated stock data
clean_stock_df = stock_df.withColumn(
    "row_rank",
    row_number().over(dedup_window)
).filter(col("row_rank") == 1).drop("row_rank")

print("\nCleaned stock data (duplicates removed):")
clean_stock_df.filter(col("symbol") == "AAPL").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example 2: Latest values and first/last functions
# Find latest performance data and track progress

window_employee_time = Window.partitionBy("employee").orderBy("month")
window_employee_full = Window.partitionBy("employee").orderBy("month").rowsBetween(
    Window.unboundedPreceding, Window.unboundedFollowing
)

employee_progress = performance_df.select(
    "employee", "month", "performance_score", "revenue_generated",
    first_value("performance_score").over(window_employee_time).alias("first_score"),
    last_value("performance_score").over(window_employee_full).alias("latest_score"),
    first_value("revenue_generated").over(window_employee_time).alias("first_revenue"),
    last_value("revenue_generated").over(window_employee_full).alias("latest_revenue")
).withColumn(
    "score_improvement",
    col("latest_score") - col("first_score")
).withColumn(
    "revenue_improvement",
    round(col("latest_revenue") - col("first_revenue"), 2)
)

print("Employee Progress Tracking:")
employee_progress.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Performance Optimization and Cost Considerations
# 
# Window functions can be **expensive operations** if not used carefully. Understanding their cost implications and optimization techniques is crucial for production systems.
# 
# ### Performance Characteristics:
# - **Memory intensive**: Each window requires sorting and buffering
# - **Shuffle operations**: Partitioning can cause data movement
# - **Non-parallelizable**: Some window operations cannot be fully parallelized
# 
# ### Optimization Strategies:
# 1. **Minimize partitions**: Use the fewest partition columns necessary
# 2. **Optimize ordering**: Choose efficient sort columns
# 3. **Frame specification**: Use precise frames to limit memory usage
# 4. **Pre-filtering**: Filter data before applying window functions
# 5. **Avoid complex expressions**: Keep window calculations simple
# 6. **Consider alternatives**: Sometimes joins or aggregations are more efficient

# CELL ********************

# Performance monitoring and query plan analysis

# Example of how to analyze query plans for window function performance
print("Query Plan Analysis:")
print("To analyze window function performance, use:")
print("1. df.explain() - See physical plan")
print("2. df.explain(True) - See all plan phases")
print("3. Check for Exchange/Sort operations")
print("4. Monitor memory usage in Spark UI")

# Simple example
simple_window_query = sales_df.withColumn(
    "rank",
    row_number().over(Window.partitionBy("region").orderBy(desc("amount")))
)

print("\nSample query plan:")
simple_window_query.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Example: Performance optimization techniques

# BAD: Multiple window functions with different specifications
#print("INEFFICIENT approach (multiple different windows):")
inefficient_query = sales_df.select(
    "salesperson", "sale_date", "amount",
    row_number().over(Window.partitionBy("salesperson").orderBy("sale_date")).alias("rank1"),
    sum("amount").over(Window.partitionBy("salesperson").orderBy("sale_date")).alias("running_total"),
    avg("amount").over(Window.partitionBy("region").orderBy("sale_date")).alias("region_avg")
)

#inefficient_query.explain()

# GOOD: Reuse window specifications and combine operations
#print("\nEFFICIENT approach (reused windows, combined operations):")

# Define reusable windows
window_salesperson = Window.partitionBy("salesperson").orderBy("sale_date")
window_region = Window.partitionBy("region").orderBy("sale_date")

efficient_query = sales_df.select(
    "salesperson", "region", "sale_date", "amount",
    # Multiple calculations over same window
    row_number().over(window_salesperson.orderBy(desc("amount"))).alias("sales_rank"),
    sum("amount").over(window_salesperson).alias("running_total"),
    count("*").over(window_salesperson).alias("days_worked"),
    # Separate window for region analysis
    avg("amount").over(window_region).alias("region_avg")
)

#efficient_query.explain()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Physical Plan Analysis Correction
# 
# ### **Both Plans Have:**
# 
# #### **Sort Operations: 3 in both**
# **INEFFICIENT:**
# 1. `Sort [region#728, sale_date#736]` (for region window)
# 2. `Sort [salesperson#727, sale_date#736]` (for running_total)  
# 3. `Sort [salesperson#727, amount#729 DESC]` (for rank)
# 
# **EFFICIENT:**
# 1. `Sort [region#728, sale_date#736]` (for region window)
# 2. `Sort [salesperson#727, sale_date#736]` (for combined window)
# 3. `Sort [salesperson#727, amount#729 DESC]` (for rank)
# 
# #### **Exchange Operations: 2 in both**
# - `hashpartitioning(salesperson#727, 200)`
# - `hashpartitioning(region#728, 200)`
# 
# ### **The Difference:**
# 
# #### **Window Operation Consolidation**
# **❌ INEFFICIENT:**
# ```
# Window [sum(amount#729) AS running_total#1571]  ← Separate
# Window [row_number() AS rank1#1569]             ← Separate
# ```
# 
# **✅ EFFICIENT:**
# ```
# Window [sum(amount#729) AS running_total#1583, 
#         count(1) AS days_worked#1585L]          ← Combined
# Window [row_number() AS sales_rank#1581]        ← Separate
# ```
# 
# ### **What Happened:**
# 
# What happened is that Spark **combined two window operations that had exactly the same specification**:
# - `sum("amount").over(window_salesperson)` 
# - `count("*").over(window_salesperson)`
# 
# Both use: `partitionBy("salesperson").orderBy("sale_date")`
# 
# ### **Benefit:**
# 
# - **Fewer data passes**: A single window operation processes both sum() and count()
# - **Less window setup overhead**: One window setup instead of two
# - **Slight memory improvement**: Avoids duplicating buffers for the same window spec


# MARKDOWN ********************

# ## Practical Exercises
# 
# Now it's time to practice what you've learned! Complete these exercises using the datasets we've created.
# 
# ### Exercise 1: Sales Analysis
# Using the sales_df, create queries to answer these questions:
# 
# 1. **Find the top 2 salespeople by total sales in each region**
# 2. **Calculate each salesperson's percentage contribution to their region's total sales**
# 3. **Identify days when each salesperson performed better than their personal average**

# CELL ********************

# Exercise 1.1: Top 2 salespeople by total sales in each region
# Your code here:

window_region_total = Window.partitionBy("region").orderBy(desc("total_sales"))

exercise_1_1 = sales_df.groupBy("salesperson", "region") \
    .agg(sum("amount").alias("total_sales")) \
    .withColumn(
        "region_rank",
        row_number().over(window_region_total)
    ).filter(col("region_rank") <= 2)

print("Exercise 1.1 - Top 2 salespeople by region:")
exercise_1_1.orderBy("region", "region_rank").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.2: Calculate percentage contribution to region total
# Your code here:

window_region_all = Window.partitionBy("region")

exercise_1_2 = sales_df.groupBy("salesperson", "region") \
    .agg(sum("amount").alias("personal_total")) \
    .withColumn(
        "region_total",
        sum("personal_total").over(window_region_all)
    ).withColumn(
        "percentage_contribution",
        round((col("personal_total") / col("region_total") * 100), 2)
    )

print("Exercise 1.2 - Percentage contribution by region:")
exercise_1_2.orderBy("region", desc("percentage_contribution")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.3: Days when salesperson performed better than personal average
# Your code here:

window_personal = Window.partitionBy("salesperson")

exercise_1_3 = sales_df.withColumn(
    "personal_average",
    avg("amount").over(window_personal)
).filter(col("amount") > col("personal_average")) \
.select(
    "salesperson", "sale_date", "amount", 
    round(col("personal_average"), 2).alias("personal_average"),
    round((col("amount") - col("personal_average")), 2).alias("above_average_by")
)

print("Exercise 1.3 - Days performing above personal average:")
exercise_1_3.orderBy("salesperson", "sale_date").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 2: Time Series Analysis
# 
# Using the stock_df, create advanced time series analytics:
# 
# 1. **Calculate 3-day and 5-day moving averages for closing prices**
# 2. **Identify the biggest price jump (% increase) from one day to the next**
# 3. **Find consecutive days where stock price increased**

# CELL ********************

# Exercise 2.1: Moving averages
# Your code here:

window_symbol_date = Window.partitionBy("symbol").orderBy("date")
window_3day = window_symbol_date.rowsBetween(-2, Window.currentRow)
window_5day = window_symbol_date.rowsBetween(-4, Window.currentRow)

exercise_2_1 = clean_stock_df.select(
    "symbol", "date", "close_price",
    round(avg("close_price").over(window_3day), 2).alias("ma_3day"),
    round(avg("close_price").over(window_5day), 2).alias("ma_5day")
)

print("Exercise 2.1 - Moving averages:")
exercise_2_1.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.2: Biggest price jump
# Your code here:

exercise_2_2 = clean_stock_df.withColumn(
    "prev_close",
    lag("close_price", 1).over(window_symbol_date)
).withColumn(
    "price_jump_pct",
    round(((col("close_price") - col("prev_close")) / col("prev_close") * 100), 2)
).filter(col("prev_close").isNotNull()) \
.orderBy(desc("price_jump_pct")) \
.limit(5)

print("Exercise 2.2 - Biggest price jumps:")
exercise_2_2.select("symbol", "date", "close_price", "prev_close", "price_jump_pct").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 2.3: Consecutive increasing days
# Your code here:

exercise_2_3 = clean_stock_df.withColumn(
    "prev_close",
    lag("close_price", 1).over(window_symbol_date)
).withColumn(
    "price_increased",
    when(col("close_price") > col("prev_close"), 1).otherwise(0)
).withColumn(
    "prev_increased",
    lag("price_increased", 1).over(window_symbol_date)
).withColumn(
    "consecutive_increase",
    when((col("price_increased") == 1) & (col("prev_increased") == 1), True).otherwise(False)
).filter(col("consecutive_increase") == True)

print("Exercise 2.3 - Consecutive increasing days:")
exercise_2_3.select("symbol", "date", "close_price", "prev_close").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Key Takeaways
# 
# Congratulations! You've mastered Window Functions in Spark. Let's summarize the key concepts and best practices:
# 
# ### **Core Window Function Concepts**:
# - **Window Specification**: `partitionBy()`, `orderBy()`, and frame specification
# - **Ranking Functions**: `row_number()`, `rank()`, `dense_rank()` for different ranking needs
# - **Lag/Lead**: Access previous/next row values for trend analysis
# - **Aggregate Functions**: `sum()`, `avg()`, `count()` over window frames
# - **Frame Specifications**: Control which rows are included in calculations
# 
# ### **Common Use Cases Mastered**:
# 1. **Data Quality**: Deduplication and finding latest records
# 2. **Time Series Analysis**: Moving averages, trend detection, period-over-period changes
# 3. **Ranking and Top-N**: Finding top performers within groups
# 4. **Running Calculations**: Cumulative sums, running totals
# 5. **Comparative Analysis**: Comparing values within partitions
# 
# ### **Performance Best Practices**:
# 1. **Reuse window specifications** to reduce computation
# 2. **Minimize partitions** to reduce shuffle operations
# 3. **Use precise frame specifications** to limit memory usage
# 4. **Filter data early** before applying window functions
# 5. **Monitor query plans** for Exchange/Sort operations
# 6. **Consider alternatives** for simple aggregations
# 
# ### **Frame Specification Patterns**:
# - `UNBOUNDED PRECEDING TO CURRENT ROW`: Running totals
# - `n PRECEDING TO CURRENT ROW`: Moving averages
# - `CURRENT ROW TO UNBOUNDED FOLLOWING`: Reverse cumulative
# - `UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING`: Entire partition
# 
# ### **When to Use Window Functions**:
# - **Use when**: You need row-level results with group context
# - **Use when**: Ranking, moving averages, or lag/lead analysis
# - **Use when**: Deduplication or finding latest records
# - **Avoid when**: Simple aggregations without row details are sufficient
# - **Avoid when**: Working with very large partitions (memory issues)

