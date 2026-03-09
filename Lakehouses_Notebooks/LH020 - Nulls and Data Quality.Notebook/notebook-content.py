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

# # LH020 🔶 Nulls and Data Quality
# ## Fundamentals Module - Lesson 4
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the fourth lesson on this module, about data quality in Apache Spark! Understanding how to detect, handle, and prevent null values is crucial for any data engineer working with real-world datasets.
# 
# ### What you'll learn in this lesson:
# - **Detecting null and NaN values** - Identifying missing and invalid data in your datasets
# - **Fill, replace, or drop strategies** - Different approaches to handle missing data
# - **Simple constraints and validation** - Implementing basic data quality rules
# - **Quality flags and monitoring** - Tracking data quality issues
# - **Error reports and alerts** - Creating comprehensive data quality reports
# - **Read and write optimization** - File format choices that minimize null-related issues
# 
# ### Prerequisites:
# - Basic understanding of DataFrames in Spark
# - Familiarity with PySpark functions
# - Understanding of data types and schemas


# MARKDOWN ********************

# ## Setting Up Our Environment
# 
# Let's start by importing the necessary libraries and creating sample datasets with various data quality issues that we'll encounter in real-world scenarios.

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import math

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating Sample Datasets with Quality Issues
# 
# For our data quality demonstration, we'll create realistic datasets with common data quality problems:
# - **Customer Data**: Missing contact information and invalid emails
# - **Sales Data**: Missing amounts and negative values
# - **Product Catalog**: Incomplete product information
# 
# These datasets simulate real-world data quality challenges you'll encounter.

# CELL ********************

# Create Customer Data with quality issues
customers_data = [
    (1, "Alice Johnson", "alice@email.com", "555-0101", "2023-01-15", 25),
    (2, "Bob Smith", None, "555-0102", "2023-02-20", 32),  # Missing email
    (3, "Carol Davis", "invalid-email", None, "2023-03-10", None),  # Invalid email, missing phone and age
    (4, "David Wilson", "david@email.com", "555-0104", None, 45),  # Missing registration date
    (5, None, "mystery@email.com", "555-0105", "2023-04-05", 28),  # Missing name
    (6, "Emma Brown", "emma@email.com", "555-0106", "2023-05-12", -5),  # Invalid age
    (7, "Frank Miller", "frank@email.com", "", "2023-06-08", 38),  # Empty phone
    (8, "Grace Wilson", "grace@email.com", "555-0108", "2023-07-15", 150)  # Unrealistic age
]

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("age", IntegerType(), True)
])

customers_df = spark.createDataFrame(customers_data, customers_schema)

print("Customers DataFrame with quality issues:")
customers_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Sales Data with missing and invalid values
sales_data = [
    (101, 1, "2023-01-20", 150.50, "completed", "laptop"),
    (102, 2, "2023-01-21", None, "pending", "mouse"),  # Missing amount
    (103, 3, None, 89.99, "completed", None),  # Missing date and product
    (104, None, "2023-01-23", 245.00, "cancelled", "monitor"),  # Missing customer_id
    (105, 4, "2023-01-24", -50.00, "completed", "keyboard"),  # Negative amount
    (106, 5, "2023-01-25", 0.0, "completed", "software"),  # Zero amount
    (107, 6, "2023-01-26", float('nan'), "pending", "webcam"),  # NaN amount
    (108, 7, "", 75.25, "completed", "headset")  # Empty date
]

sales_schema = StructType([
    StructField("sale_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("sale_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("product_category", StringType(), True)
])

sales_df = spark.createDataFrame(sales_data, sales_schema)

print("Sales DataFrame with quality issues:")
sales_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Detecting Null and NaN Values
# 
# The first step in data quality management is identifying where the problems are. Let's explore different methods to detect null and NaN values in our datasets.

# CELL ********************

# Method 1: Basic null detection using isnull() and isnan()
print("1- Basic Null Detection ")

# Check for nulls in specific columns
null_emails = customers_df.filter(col("email").isNull()).count()
null_names = customers_df.filter(col("name").isNull()).count()
null_ages = customers_df.filter(col("age").isNull()).count()

print(f"Customers with null emails: {null_emails}")
print(f"Customers with null names: {null_names}")
print(f"Customers with null ages: {null_ages}")

# Check for NaN values in numeric columns
nan_amounts = sales_df.filter(isnan(col("amount"))).count()
print(f"Sales with NaN amounts: {nan_amounts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Method 2: Comprehensive quality summary function
def get_quality_summary(df, df_name):
    """Generate a comprehensive data quality summary for a DataFrame"""
    
    total_rows = df.count()
    print(f"\n2- Data Quality Summary: {df_name}")
    print(f"Total rows: {total_rows}")
    print("\nNull/Missing Value Analysis:")
    
    # Check each column for nulls
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        null_percentage = (null_count / total_rows) * 100
        
        # Also check for empty strings
        empty_count = df.filter(col(column) == "").count()
        
        # Check for NaN in numeric columns
        nan_count = 0
        column_type = dict(df.dtypes)[column]
        if column_type in ['double', 'float', 'int', 'bigint']:
            try:
                nan_count = df.filter(isnan(col(column))).count()
            except:
                pass
        
        total_issues = null_count + empty_count + nan_count
        
        if total_issues > 0:
            print(f"  {column}: {null_count} nulls ({null_percentage:.1f}%), {empty_count} empty, {nan_count} NaN")
        else:
            print(f"  {column}: ✓ Clean")

# Apply quality summary to our datasets
get_quality_summary(customers_df, "Customers")
get_quality_summary(sales_df, "Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Method 3: Visual null pattern analysis
print("3- Null Pattern Analysis")

# Create flags for missing data
customers_with_flags = customers_df.select(
    "customer_id",
    "name",
    col("name").isNull().alias("missing_name"),
    col("email").isNull().alias("missing_email"),
    col("phone").isNull().alias("missing_phone"),
    col("age").isNull().alias("missing_age")
)

print("Missing data patterns:")
customers_with_flags.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Fill, Replace, or Drop Strategies
# 
# Once we've identified data quality issues, we need to decide how to handle them. Let's explore the three main strategies: filling, replacing, and dropping problematic data.

# CELL ********************

# Strategy 1: Filling missing values
print("1- Fill Strategies")

# Fill with calculated values (like mean, median)
age_stats = customers_df.select(
    mean("age").alias("mean_age"),
    expr("percentile_approx(age, 0.5)").alias("median_age")
).collect()[0]

print(f"\nAge statistics - Mean: {age_stats['mean_age']:.1f}, Median: {age_stats['median_age']}")

# Fill with default values
customers_filled = customers_df \
    .fillna("Unknown", ["name"]) \
    .fillna("no-email@company.com", ["email"]) \
    .fillna("000-0000", ["phone"]) \
    .fillna(30, ["age"])  # Fill age with median/average

print("Customers after filling nulls:")
customers_filled.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

sales_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Strategy 2: Replace invalid values
print("2- Replace Strategies")

# Replace empty strings with nulls first, then handle
sales_cleaned = sales_df \
    .withColumn("sale_date", when(col("sale_date") == "", None).otherwise(col("sale_date"))) \
    .withColumn("amount", 
        when(col("amount") < 0, 0)  # Replace negative amounts with 0
        .when(isnan(col("amount")), None)  # Replace NaN with null
        .otherwise(col("amount"))
    )

print("Sales after replacing invalid values:")
sales_cleaned.show()

# Replace based on business logic
customers_validated = customers_df \
    .withColumn("age", 
        when((col("age") < 0) | (col("age") > 120), None)  # Invalid ages become null
        .otherwise(col("age"))
    ) \
    .withColumn("email_valid", 
        when(col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$'), True)
        .otherwise(False)
    )

print("\nCustomers with validation flags:")
customers_validated.select("customer_id", "name", "email", "email_valid", "age").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from functools import reduce
from operator import add

# Strategy 3: Drop problematic records
print("3- Drop Strategies")

# Drop rows with any nulls
customers_complete = customers_df.dropna()
print(f"Complete customers (no nulls): {customers_complete.count()} of {customers_df.count()}")

# Drop rows with nulls in specific critical columns
customers_with_contact = customers_df.dropna(subset=["name", "email"])
print(f"Customers with name and email: {customers_with_contact.count()} of {customers_df.count()}")

# Drop rows based on threshold (ex. more than 50% missing)
def drop_by_missing_threshold(df, threshold=0.5):
    """Drop rows that have more than threshold proportion of missing values"""
    total_cols = len(df.columns)
    max_nulls = int(total_cols * threshold)
    
    # Count nulls per row
    null_counts = df.select(
        *df.columns,
        reduce(add, [col(c).isNull().cast("int") for c in df.columns]).alias("null_count")
    )
    
    # Filter out rows with too many nulls
    return null_counts.filter(col("null_count") <= max_nulls).drop("null_count")

customers_threshold = drop_by_missing_threshold(customers_df, 0.3)
print(f"Customers after threshold filter: {customers_threshold.count()} of {customers_df.count()}")

print("\nCustomers remaining after threshold filter:")
customers_threshold.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Simple Constraints and Validation
# 
# Implementing data constraints helps ensure data quality at ingestion time. Let's create validation functions that can check our data against business rules.

# CELL ********************

# Define constraint functions
def validate_customer_constraints(df):
    """Validate customer data against business constraints"""
    
    print("Customer Data Constraints Validation")
    
    # Required fields check
    required_fields = ["customer_id", "name"]
    for field in required_fields:
        null_count = df.filter(col(field).isNull()).count()
        if null_count > 0:
            print(f"❌ CONSTRAINT VIOLATION: {field} has {null_count} null values (REQUIRED)")
        else:
            print(f"✅ {field}: All values present")
    
    # Age constraints
    invalid_ages = df.filter(
        (col("age") < 0) | (col("age") > 120) | col("age").isNull()
    ).count()
    
    if invalid_ages > 0:
        print(f"❌ CONSTRAINT VIOLATION: {invalid_ages} records with invalid age (must be 0-120)")
    else:
        print("✅ Age: All values valid")
    
    # Email format validation
    invalid_emails = df.filter(
        col("email").isNotNull() & 
        ~col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')
    ).count()
    
    if invalid_emails > 0:
        print(f"❌ CONSTRAINT VIOLATION: {invalid_emails} records with invalid email format")
    else:
        print("✅ Email: All formats valid")
    
    # Unique customer_id constraint
    total_rows = df.count()
    unique_ids = df.select("customer_id").distinct().count()
    
    if total_rows != unique_ids:
        print(f"❌ CONSTRAINT VIOLATION: Duplicate customer_ids found ({total_rows} rows, {unique_ids} unique)")
    else:
        print("✅ Customer ID: All values unique")

# Run validation
validate_customer_constraints(customers_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a validation function for sales data
def validate_sales_constraints(df):
    """Validate sales data against business constraints"""
    
    print("\nSales Data Constraints Validation")
    
    # Required fields
    required_fields = ["sale_id", "customer_id", "amount"]
    for field in required_fields:
        null_count = df.filter(col(field).isNull()).count()
        if null_count > 0:
            print(f"❌ CONSTRAINT VIOLATION: {field} has {null_count} null values (REQUIRED)")
        else:
            print(f"✅ {field}: All values present")
    
    # Amount constraints
    invalid_amounts = df.filter(
        (col("amount") <= 0) | col("amount").isNull() | isnan(col("amount"))
    ).count()
    
    if invalid_amounts > 0:
        print(f"❌ CONSTRAINT VIOLATION: {invalid_amounts} records with invalid amount (must be > 0)")
    else:
        print("✅ Amount: All values valid")
    
    # Status validation
    valid_statuses = ["completed", "pending", "cancelled"]
    invalid_status = df.filter(
        ~col("status").isin(valid_statuses) | col("status").isNull()
    ).count()
    
    if invalid_status > 0:
        print(f"❌ CONSTRAINT VIOLATION: {invalid_status} records with invalid status")
        print(f"   Valid statuses: {valid_statuses}")
    else:
        print("✅ Status: All values valid")

# Run sales validation
validate_sales_constraints(sales_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Quality Flags and Monitoring
# 
# Instead of immediately fixing or dropping bad data, we can add quality flags to track issues while preserving the original data for analysis.

# CELL ********************

# Create quality flags for customers
customers_with_quality_flags = customers_df \
    .withColumn("has_missing_name", col("name").isNull()) \
    .withColumn("has_missing_email", col("email").isNull()) \
    .withColumn("has_invalid_email", 
        col("email").isNotNull() & ~col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$')
    ) \
    .withColumn("has_missing_phone", col("phone").isNull() | (col("phone") == "")) \
    .withColumn("has_invalid_age", 
        col("age").isNull() | (col("age") < 0) | (col("age") > 120)
    ) \
    .withColumn("quality_score", 
        100 - (
            col("has_missing_name").cast("int") * 30 +
            col("has_missing_email").cast("int") * 25 +
            col("has_invalid_email").cast("int") * 20 +
            col("has_missing_phone").cast("int") * 15 +
            col("has_invalid_age").cast("int") * 10
        )
    ) \
    .withColumn("quality_tier",
        when(col("quality_score") >= 90, "HIGH")
        .when(col("quality_score") >= 70, "MEDIUM")
        .otherwise("LOW")
    )

print("Customers with quality flags and scores:")
customers_with_quality_flags.select(
    "customer_id", "name", "email", "age",
    "has_missing_name", "has_invalid_email", "has_invalid_age",
    "quality_score", "quality_tier"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Quality monitoring aggregations
print("Quality Monitoring Dashboard")

# Overall quality distribution
quality_distribution = customers_with_quality_flags \
    .groupBy("quality_tier") \
    .agg(
        count("*").alias("record_count"),
        avg("quality_score").alias("avg_score")
    ) \
    .orderBy("quality_tier")

print("Quality tier distribution:")
quality_distribution.show()

# Detailed issue breakdown
issue_summary = customers_with_quality_flags.agg(
    sum(col("has_missing_name").cast("int")).alias("missing_names"),
    sum(col("has_missing_email").cast("int")).alias("missing_emails"),
    sum(col("has_invalid_email").cast("int")).alias("invalid_emails"),
    sum(col("has_missing_phone").cast("int")).alias("missing_phones"),
    sum(col("has_invalid_age").cast("int")).alias("invalid_ages"),
    avg(col("quality_score")).alias("overall_avg_score")
)

print("\nDetailed issue breakdown:")
issue_summary.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Error Reports and Alerts
# 
# Creating comprehensive error reports helps teams understand and address data quality issues systematically.

# CELL ********************

# Generate error report
def generate_quality_report(df, df_name):
    """Generate a comprehensive data quality report"""
    
    print(f"\n{'='*50}")
    print(f"DATA QUALITY REPORT: {df_name}")
    print(f"Generated at: {datetime.now()}")
    print(f"{'='*50}")
    
    total_records = df.count()
    print(f"Total Records: {total_records}")
    
    # Schema information
    print(f"\nSCHEMA INFORMATION:")
    for field in df.schema.fields:
        nullable = "NULL" if field.nullable else "NOT NULL"
        print(f"  {field.name}: {field.dataType} ({nullable})")
    
    # Detailed quality issues
    print(f"\nDETAILED QUALITY ISSUES:")
    
    issues_found = False
    for column in df.columns:
        # Count various types of issues
        null_count = df.filter(col(column).isNull()).count()
        empty_count = df.filter(col(column) == "").count()
        
        # Check for NaN in numeric columns
        nan_count = 0
        try:
            df.select(column).dtypes[0][1]
            if 'double' in str(df.schema[column].dataType).lower():
                nan_count = df.filter(isnan(col(column))).count()
        except:
            pass
        
        total_issues = null_count + empty_count + nan_count
        
        if total_issues > 0:
            issues_found = True
            percentage = (total_issues / total_records) * 100
            print(f"  ❌ {column}:")
            if null_count > 0:
                print(f"      - {null_count} NULL values ({(null_count/total_records)*100:.1f}%)")
            if empty_count > 0:
                print(f"      - {empty_count} EMPTY values ({(empty_count/total_records)*100:.1f}%)")
            if nan_count > 0:
                print(f"      - {nan_count} NaN values ({(nan_count/total_records)*100:.1f}%)")
    
    if not issues_found:
        print("  ✅ No data quality issues detected!")
    
    return total_records

# Generate reports for our datasets
generate_quality_report(customers_df, "Customers")
generate_quality_report(sales_df, "Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create alerts for critical quality issues
def check_quality_alerts(df, df_name, thresholds=None):
    """Check for quality issues that exceed defined thresholds"""
    
    if thresholds is None:
        thresholds = {
            'max_null_percentage': 10.0,  # Alert if any column has >10% nulls
            'max_overall_null_percentage': 5.0,  # Alert if overall nulls >5%
            'min_records': 1  # Alert if less than 1 record
        }
    
    print(f"\nQUALITY ALERTS: {df_name.upper()}")
    
    total_records = df.count()
    alerts_triggered = []
    
    # Check minimum records threshold
    if total_records < thresholds['min_records']:
        alerts_triggered.append(f"CRITICAL: Record count ({total_records}) below threshold ({thresholds['min_records']})")
    
    # Check individual column null percentages
    total_null_count = 0
    for column in df.columns:
        null_count = df.filter(col(column).isNull()).count()
        total_null_count += null_count
        
        if total_records > 0:
            null_percentage = (null_count / total_records) * 100
            if null_percentage > thresholds['max_null_percentage']:
                alerts_triggered.append(
                    f"HIGH NULL RATE: {column} has {null_percentage:.1f}% nulls (threshold: {thresholds['max_null_percentage']}%)"
                )
    
    # Check overall null percentage
    total_cells = total_records * len(df.columns)
    if total_cells > 0:
        overall_null_percentage = (total_null_count / total_cells) * 100
        if overall_null_percentage > thresholds['max_overall_null_percentage']:
            alerts_triggered.append(
                f"HIGH OVERALL NULL RATE: {overall_null_percentage:.1f}% of all cells are null (threshold: {thresholds['max_overall_null_percentage']}%)"
            )
    
    # Display alerts
    if alerts_triggered:
        for alert in alerts_triggered:
            print(f"  🚨 {alert}")
        print(f"\n  Summary: {len(alerts_triggered)} alert(s) triggered")
    else:
        print("  ✅ All quality checks passed - no alerts triggered")
    
    return alerts_triggered

# Check alerts for our datasets
customer_alerts = check_quality_alerts(customers_df, "Customers")
sales_alerts = check_quality_alerts(sales_df, "Sales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Read and Write Choices that Reduce Nulls
# 
# The file formats and options we choose when reading and writing data can significantly impact null handling. Let's explore best practices for different scenarios.

# CELL ********************

# File format recommendations and examples
print("File Format Best Practices for Null Handling")

# 1. Parquet - Best for preserving schema and null information
print("\n1- PARQUET (Recommended for most use cases):")
print("   ✅ Preserves exact schema and null information")
print("   ✅ Efficient storage and query performance")
print("   ✅ Built-in compression")
print("   ❌ Not human-readable")

# Write example with schema preservation
customers_df.write \
    .mode("overwrite") \
    .option("compression", "snappy") \
    .parquet("Files/customers_with_nulls.parquet")

# Read back and verify schema preservation
customers_from_parquet = spark.read.parquet("Files/customers_with_nulls.parquet")
print(f"\n   Records with nulls preserved: {customers_from_parquet.filter(col('name').isNull()).count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CSV handling - common pitfalls and solutions
print("\n2- CSV FILES (Use with caution):")
print("   ❌ No built-in schema preservation")
print("   ❌ Null handling depends on options")
print("   ✅ Human-readable and widely supported")
print("   ✅ Good for data exchange")

# Demonstrate CSV null handling options
print("\nCSV Null Handling Options:")

# Write CSV with explicit null value
customers_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("nullValue", "NULL") \
    .csv("Files/customers_explicit_nulls.csv")

# Read back with proper null handling
customers_from_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("nullValue", "NULL") \
    .option("emptyValue", None) \
    .csv("Files/customers_explicit_nulls.csv")

print(f"Records with nulls after CSV round-trip: {customers_from_csv.filter(col('name').isNull()).count()}")

# Show the difference with default CSV reading
customers_default_csv = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("Files/customers_explicit_nulls.csv")

print(f"Records with nulls using default CSV options: {customers_default_csv.filter(col('name').isNull()).count()}")

print("\nRecommended CSV read options:")
print("  - nullValue='NULL' or nullValue=''")
print("  - emptyValue=None")
print("  - inferSchema=true (but validate results)")
print("  - enforceSchema=true (if you have a predefined schema)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(customers_default_csv)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Schema enforcement to prevent null issues
print("\nSchema Enforcement for Quality Control")

# Define a strict schema with non-nullable fields
strict_customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),  # NOT NULL
    StructField("name", StringType(), False),          # NOT NULL
    StructField("email", StringType(), True),          # Nullable
    StructField("phone", StringType(), True),          # Nullable
    StructField("registration_date", StringType(), True), # Nullable
    StructField("age", IntegerType(), True)            # Nullable
])

print("Strict schema defined:")
for field in strict_customer_schema.fields:
    nullable = "NULLABLE" if field.nullable else "NOT NULL"
    print(f"  {field.name}: {field.dataType} ({nullable})")

print("\nCurrent Customer Data:")
customers_df.show()

# Try to create DataFrame with strict schema - this will show validation
try:
    strict_customers_df = spark.createDataFrame(customers_data, strict_customer_schema)
    print("\n✅ Data fits strict schema")
except Exception as e:
    print(f"\n❌ Schema enforcement failed: {str(e)}")

# Write with schema information preserved
customers_df.write \
    .mode("overwrite") \
    .option("mergeSchema", "false") \
    .option("compression", "snappy") \
    .parquet("Files/customers_with_schema.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Practical Exercises
# 
# Now it's time to practice what you've learned! Complete these exercises to reinforce your understanding of null handling and data quality.

# MARKDOWN ********************

# ### Exercise 1: Data Quality Assessment
# 
# Using the datasets we've created, complete these tasks:
# 
# 1. **Create a quality score function** that rates records from 0-100 based on completeness
# 2. **Find records that need immediate attention** (quality score < 65)
# 3. **Calculate the percentage of 'clean' records** (no nulls or invalid values) in each dataset

# CELL ********************

# Exercise 1.1: Create a quality score function for sales data
# Your code here:

sales_with_quality = sales_df \
    .withColumn("missing_customer_id", col("customer_id").isNull()) \
    .withColumn("missing_date", col("sale_date").isNull() | (col("sale_date") == "")) \
    .withColumn("invalid_amount", col("amount").isNull() | isnan(col("amount")) | (col("amount") <= 0)) \
    .withColumn("missing_product", col("product_category").isNull()) \
    .withColumn("sales_quality_score",
        100 - (
            col("missing_customer_id").cast("int") * 40 +
            col("missing_date").cast("int") * 25 +
            col("invalid_amount").cast("int") * 25 +
            col("missing_product").cast("int") * 10
        )
    )

print("Exercise 1.1 - Sales with quality scores:")
sales_with_quality.select(
    "sale_id", "customer_id", "amount", "product_category",
    "missing_customer_id", "invalid_amount", "sales_quality_score"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.2: Find records needing immediate attention
# Your code here:

critical_customers = customers_with_quality_flags.filter(col("quality_score") < 65)
critical_sales = sales_with_quality.filter(col("sales_quality_score") < 65)

print("Exercise 1.2 - Records needing immediate attention:")
print(f"\nCritical customers (quality < 65): {critical_customers.count()}")
critical_customers.select("customer_id", "name", "email", "quality_score").show()

print(f"\nCritical sales (quality < 65): {critical_sales.count()}")
critical_sales.select("sale_id", "customer_id", "amount", "sales_quality_score").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exercise 1.3: Calculate percentage of clean records
# Your code here:

# Define "clean" as having no nulls in critical fields
clean_customers = customers_df.filter(
    col("customer_id").isNotNull() & 
    col("name").isNotNull() & 
    col("email").isNotNull() & 
    col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$') &
    col("age").isNotNull() & 
    (col("age") >= 0) & 
    (col("age") <= 120)
)

clean_sales = sales_df.filter(
    col("sale_id").isNotNull() & 
    col("customer_id").isNotNull() & 
    col("sale_date").isNotNull() & 
    (col("sale_date") != "") &
    col("amount").isNotNull() & 
    ~isnan(col("amount")) & 
    (col("amount") > 0)
)

customer_clean_percentage = (clean_customers.count() / customers_df.count()) * 100
sales_clean_percentage = (clean_sales.count() / sales_df.count()) * 100

print("Exercise 1.3 - Clean records percentage:")
print(f"Customers: {clean_customers.count()}/{customers_df.count()} ({customer_clean_percentage:.1f}% clean)")
print(f"Sales: {clean_sales.count()}/{sales_df.count()} ({sales_clean_percentage:.1f}% clean)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 2: Data Cleaning Pipeline
# 
# Create a comprehensive data cleaning pipeline that:
# 
# 1. **Applies appropriate cleaning strategies** based on the data quality assessment
# 2. **Preserves original data** with quality flags
# 3. **Creates a 'production-ready' clean dataset** for downstream systems

# CELL ********************

# Exercise 2: Create a complete data cleaning pipeline
# Your code here:

def clean_customers_pipeline(df):
    """Complete customer data cleaning pipeline"""
    
    # Step 1: Add quality flags (preserve original)
    df_with_flags = df \
        .withColumn("original_name", col("name")) \
        .withColumn("original_email", col("email")) \
        .withColumn("original_age", col("age"))
    
    # Step 2: Apply cleaning rules
    cleaned_df = df_with_flags \
        .withColumn("name", 
            when(col("name").isNull(), "Unknown Customer")
            .otherwise(col("name"))
        ) \
        .withColumn("email", 
            when(col("email").isNull() | ~col("email").rlike(r'^[^@]+@[^@]+\.[^@]+$'), 
                 "noreply@company.com")
            .otherwise(col("email"))
        ) \
        .withColumn("age", 
            when((col("age") < 0) | (col("age") > 120) | col("age").isNull(), 30)
            .otherwise(col("age"))
        ) \
        .withColumn("phone", 
            when(col("phone").isNull() | (col("phone") == ""), "000-0000")
            .otherwise(col("phone"))
        ) \
        .withColumn("cleaned_timestamp", current_timestamp())
    
    return cleaned_df

# Apply the cleaning pipeline
cleaned_customers = clean_customers_pipeline(customers_df)

print("Exercise 2 - Cleaned customers with original data preserved:")
cleaned_customers.select(
    "customer_id", "name", "original_name", "email", "original_email", 
    "age", "original_age"
).show()

# Create production-ready dataset (only clean fields)
production_customers = cleaned_customers.select(
    "customer_id", "name", "email", "phone", "registration_date", "age", "cleaned_timestamp"
)

print("\nProduction-ready customer dataset:")
production_customers.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Key Takeaways
# 
# Congratulations! You've completed the Nulls and Data Quality lesson. Let's summarize the essential concepts you've mastered:
# 
# ### **Detection Techniques Mastered**:
# - **Null detection**: Using `isnull()` and `isNotNull()` for missing values
# - **NaN detection**: Using `isnan()` for invalid numeric values  
# - **Quality assessment**: Creating quality summary functions
# - **Pattern analysis**: Identifying missing data patterns across columns
# 
# ### **Handling Strategies**:
# - **Fill strategies**: Default values, calculated means/medians, business rules
# - **Replace strategies**: Converting invalid values, format standardization
# - **Drop strategies**: Removing incomplete records, threshold-based filtering
# - **Constraint validation**: Implementing business rules and data validation
# 
# ### **Quality Monitoring**:
# - **Quality flags**: Tracking issues without losing original data
# - **Quality scoring**: Numerical assessment of record completeness
# - **Alert systems**: Automated threshold-based quality monitoring
# - **Comprehensive reporting**: Detailed quality dashboards and summaries
# 
# ### **File Format Best Practices**:
# - **Parquet**: Best for schema preservation and null handling
# - **CSV considerations**: Proper options for null value handling
# - **Schema enforcement**: Preventing quality issues at ingestion
# 
# ### **Production-Ready Patterns**:
# 1. **Assess first**: Always understand your data quality before cleaning
# 2. **Preserve originals**: Keep original values when applying transformations
# 3. **Flag don't fix**: Use quality flags to track issues without data loss
# 4. **Monitor continuously**: Implement ongoing quality monitoring
# 5. **Choose appropriate strategies**: Fill, replace or drop based on business context
# 6. **Document decisions**: Track what cleaning rules were applied and why

