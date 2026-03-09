# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b0b11f9a-f4fd-43b1-a40a-02b8fea21ce6",
# META       "default_lakehouse_name": "LH_Spark_Fundamentals",
# META       "default_lakehouse_workspace_id": "16dbabe5-14e9-45db-94e1-0ee32a76bce4",
# META       "known_lakehouses": [
# META         {
# META           "id": "b0b11f9a-f4fd-43b1-a40a-02b8fea21ce6"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # LH019 🟢 Data Formatting: Dates, Strings and Conditionals
# ## Fundamentals Module - Lesson 3
# 
# >  **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute. 
# 
# Welcome to the third lesson in our Fundamentals module! In this comprehensive lesson, we'll master data formatting operations in Apache Spark, focusing on dates, strings, and conditional logic.
# 
# ### What you'll learn in this lesson:
# - **Date and timestamp formatting** - Using to_date, to_timestamp with custom formats
# - **Timezone handling** - Working with session timezones and UTC conversions
# - **Date manipulation** - date_trunc, date_format, add_months, datediff functions
# - **String operations** - trim, lower, upper, substring, split, concat functions
# - **Conditional logic** - when, otherwise, and coalesce for data transformations
# - **Safe casting** - Best practices and common pitfalls to avoid
# 
# ### Prerequisites:
# - Basic understanding of DataFrames in Spark
# - Familiarity with PySpark functions
# - Understanding of data types in Spark


# MARKDOWN ********************

# ## Setting Up Our Environment
# 
# Let's start by importing the necessary libraries and creating sample datasets with different data formatting challenges that we'll encounter in real-world scenarios.

# CELL ********************

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timezone
import pytz

print("Libraries imported successfully!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Creating Sample Datasets
# 
# For our data formatting demonstration, we'll create datasets with common data quality issues:
# - **User Data**: Contains mixed date formats, inconsistent string cases, and messy text
# - **Transaction Data**: Contains timestamps in different timezones and string formatting challenges
# - **Product Data**: Contains text data that needs cleaning and conditional transformations
# 
# These datasets will help us practice real-world data cleaning scenarios.

# CELL ********************

# Create User Data with various formatting issues
user_data = [
    (1, "  ALICE johnson  ", "2023-05-15", "alice@email.com", "new york", "  +1-555-0123  "),
    (2, "bob SMITH", "05/20/2023", "BOB@EMAIL.COM", "Los Angeles", "555.0124"),
    (3, "  carol davis", "2023.06.10", "carol@email.com", "CHICAGO", "(555) 0125"),
    (4, "David Wilson", "20230725", "david@EMAIL.com", "houston", "555-0126"),
    (5, "emma brown  ", "2023-08-30 10:30:00", "emma@email.com", "Phoenix", None),
    (6, "Frank Miller", None, "frank@email.com", "philadelphia", "555 0127")
]

user_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("phone", StringType(), True)
])

users_df = spark.createDataFrame(user_data, user_schema)

print("Users DataFrame (raw data with formatting issues):")
users_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Transaction Data with timezone and timestamp challenges
transaction_data = [
    (101, 1, "2024-01-15 09:30:00", 150.75, "USD", "completed", "US/Eastern"),
    (102, 2, "2024-01-15 14:45:00", 89.50, "USD", "completed", "US/Pacific"),
    (103, 1, "2024-01-16T08:15:30Z", 245.00, "EUR", "pending", "UTC"),
    (104, 3, "15/01/2024 16:20", 67.25, "GBP", "completed", "Europe/London"),
    (105, 4, "2024.01.17 11:45:30", 312.80, "USD", "failed", "US/Central"),
    (106, None, "2024-01-18 07:30:00", 99.99, "CAD", "refunded", "America/Toronto")
]

transaction_schema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("transaction_time", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("timezone_info", StringType(), True)
])

transactions_df = spark.createDataFrame(transaction_data, transaction_schema)

print("Transactions DataFrame (mixed timestamps and timezones):")
transactions_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create Product Data with text processing challenges
product_data = [
    (201, "  iPhone 15 Pro Max - 256GB  ", "ELECTRONICS", "$1199.99", "IN_STOCK", "apple-iphone-15-pro-max-256gb"),
    (202, "Samsung Galaxy S24 Ultra", "electronics", "1299.00 USD", "out_of_stock", "samsung-galaxy-s24-ultra"),
    (203, "MacBook Air M2 - 13 inch", "Electronics", "£1099", "IN STOCK", "macbook-air-m2-13"),
    (204, "Sony WH-1000XM5 Headphones", "AUDIO", "€399.99", "limited_stock", "sony-wh1000xm5-headphones"),
    (205, "Dell XPS 13 (2024)", "Computers", "999 dollars", "pre_order", "dell-xps-13-2024"),
    (206, "  LOGITECH MX Master 3S  ", "accessories", "$99.99", None, "logitech-mx-master-3s")
]

product_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price_text", StringType(), True),
    StructField("stock_status", StringType(), True),
    StructField("product_slug", StringType(), True)
])

products_df = spark.createDataFrame(product_data, product_schema)

print("Products DataFrame (text processing challenges):")
products_df.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 1: Date and Timestamp Formatting
# 
# Working with dates and timestamps is one of the most common challenges in data engineering. Spark provides powerful functions to parse, format, and manipulate temporal data.

# MARKDOWN ********************

# ### 1.1 Converting Strings to Dates with to_date()
# 
# The `to_date()` function converts string representations to date type. When formats vary, we need to specify custom patterns.

# CELL ********************

# Example: Converting different date formats to standard date type
print("Original user registration dates:")
users_df.select("user_id", "full_name", "registration_date").show(truncate=False)

# Convert different date formats using to_date with format patterns
users_clean_dates = users_df.select(
    "user_id",
    "full_name",
    "registration_date",
    # Standard format: Use to_date() without format for auto-detection (most reliable)
    to_date(col("registration_date")).alias("date_auto_format"),
    # For timestamp strings, convert to timestamp first, then to date
    to_date(to_timestamp(col("registration_date"))).alias("date_from_timestamp"),
    # US format: MM/dd/yyyy  
    to_date(col("registration_date"), "MM/dd/yyyy").alias("date_format_us"),
    # Dot format: yyyy.MM.dd
    to_date(col("registration_date"), "yyyy.MM.dd").alias("date_format_dot"),
    # Compact format: yyyyMMdd
    to_date(col("registration_date"), "yyyyMMdd").alias("date_format_compact"),
    # Use coalesce to get the first non-null result
    coalesce(
        to_date(col("registration_date")),
        to_date(to_timestamp(col("registration_date"))),
        to_date(col("registration_date"), "yyyy-MM-dd"),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        to_date(col("registration_date"), "yyyy.MM.dd"),
        to_date(col("registration_date"), "yyyyMMdd")
    ).alias("parsed_date")
)

print("\nDifferent date format attempts and final parsed result:")
display(users_clean_dates)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.2 Converting Strings to Timestamps with to_timestamp()
# 
# The `to_timestamp()` function is similar to `to_date()` but preserves time information and handles timezone conversions.

# CELL ********************

# Example: Converting different timestamp formats
print("Original transaction timestamps:")
transactions_df.select("transaction_id", "transaction_time", "timezone_info").show(truncate=False)

# Convert different timestamp formats using to_timestamp
transactions_clean_timestamps = transactions_df.select(
    "transaction_id",
    "transaction_time",
    "timezone_info",
    # Standard format: yyyy-MM-dd HH:mm:ss
    to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss").alias("timestamp_format_1"),
    # ISO format: yyyy-MM-dd'T'HH:mm:ss'Z'
    to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("timestamp_format_2"),
    # European format: dd/MM/yyyy HH:mm
    to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm").alias("timestamp_format_3"),
    # Dot format: yyyy.MM.dd HH:mm:ss
    to_timestamp(col("transaction_time"), "yyyy.MM.dd HH:mm:ss").alias("timestamp_format_4"),
    # Use coalesce for flexible parsing
    coalesce(
        to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm"),
        to_timestamp(col("transaction_time"), "yyyy.MM.dd HH:mm:ss")
    ).alias("parsed_timestamp")
)

print("\nDifferent timestamp format attempts and final parsed result:")
display(transactions_clean_timestamps)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.3 Timezone Handling and UTC Conversions
# 
# Working with different timezones is crucial for global applications. Spark provides functions to handle timezone conversions properly.

# CELL ********************

# Set session timezone and demonstrate timezone conversions
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"Session timezone set to: {spark.conf.get('spark.sql.session.timeZone')}")

# Create a clean transactions dataset with parsed timestamps
transactions_with_timestamps = transactions_df.select(
    "transaction_id",
    "user_id",
    "amount",
    "status",
    "timezone_info",
    coalesce(
        to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm"),
        to_timestamp(col("transaction_time"), "yyyy.MM.dd HH:mm:ss")
    ).alias("timestamp_utc")
).filter(col("timestamp_utc").isNotNull())

# Timezone conversions
timezone_conversions = transactions_with_timestamps.select(
    "transaction_id",
    "timestamp_utc",
    "timezone_info",
    # Convert UTC to different timezones
    from_utc_timestamp(col("timestamp_utc"), "US/Eastern").alias("eastern_time"),
    from_utc_timestamp(col("timestamp_utc"), "US/Pacific").alias("pacific_time"),
    from_utc_timestamp(col("timestamp_utc"), "Europe/London").alias("london_time"),
    from_utc_timestamp(col("timestamp_utc"), "Asia/Tokyo").alias("tokyo_time"),
    # Convert to UTC (if timestamp was in local timezone)
    to_utc_timestamp(col("timestamp_utc"), "US/Eastern").alias("eastern_to_utc")
)

print("\nTimezone conversions example:")
display(timezone_conversions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 1.4 Date Manipulation Functions
# 
# Spark provides powerful functions for date arithmetic, truncation, and formatting operations.

# CELL ********************

# Demonstrate date manipulation functions
date_manipulations = users_df.select(
    "user_id",
    "full_name",
    # Parse registration date using robust approach
    coalesce(
        to_date(col("registration_date")),
        to_date(to_timestamp(col("registration_date"))),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        to_date(col("registration_date"), "yyyy.MM.dd"),
        to_date(col("registration_date"), "yyyyMMdd")
    ).alias("registration_date")
).filter(col("registration_date").isNotNull())

# Add date manipulation columns
date_operations = date_manipulations.select(
    "user_id",
    "full_name", 
    "registration_date",
    # Current date
    current_date().alias("today"),
    current_timestamp().alias("now"),
    # Date formatting
    date_format(col("registration_date"), "MMM dd, yyyy").alias("formatted_date"),
    date_format(col("registration_date"), "EEEE, MMMM dd, yyyy").alias("full_format"),
    # Date truncation
    date_trunc("month", col("registration_date")).alias("month_start"),
    date_trunc("year", col("registration_date")).alias("year_start"),
    # Date arithmetic
    add_months(col("registration_date"), 6).alias("six_months_later"),
    add_months(col("registration_date"), -3).alias("three_months_before"),
    date_add(col("registration_date"), 30).alias("thirty_days_later"),
    date_sub(col("registration_date"),7 ).alias("week_before"),
    # Date differences
    datediff(current_date(), col("registration_date")).alias("days_since_registration"),
    months_between(current_date(), col("registration_date")).alias("months_since_registration"),
    # Extract date parts
    year(col("registration_date")).alias("reg_year"),
    month(col("registration_date")).alias("reg_month"),
    dayofmonth(col("registration_date")).alias("reg_day"),
    dayofweek(col("registration_date")).alias("day_of_week"),
    quarter(col("registration_date")).alias("reg_quarter")
)

print("Date manipulation examples:")
#date_operations.show(truncate=False)

display(date_operations)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 2: String Operations and Text Processing
# 
# String manipulation is essential for data cleaning and standardization. Spark provides comprehensive string functions for various text processing needs.

# MARKDOWN ********************

# ### 2.1 Basic String Cleaning Functions
# 
# Let's start with the fundamental string cleaning operations: trim, case conversions, and basic text manipulation.

# CELL ********************

# Demonstrate basic string cleaning operations
print("Original user data with string issues:")
users_df.select("user_id", "full_name", "email", "city", "phone").show(truncate=False)

# Apply string cleaning functions
users_cleaned_strings = users_df.select(
    "user_id",
    "full_name",
    # Trim whitespace
    trim(col("full_name")).alias("name_trimmed"),
    # Case conversions
    upper(col("full_name")).alias("name_upper"),
    lower(col("full_name")).alias("name_lower"),
    initcap(col("full_name")).alias("name_proper_case"),
    # Clean and standardize email
    lower(trim(col("email"))).alias("email_clean"),
    # Clean and standardize city
    initcap(trim(col("city"))).alias("city_clean"),
    # Clean phone numbers (remove extra spaces)
    trim(col("phone")).alias("phone_trimmed"),
    # Get string length
    length(trim(col("full_name"))).alias("name_length")
)

print("\nCleaned strings with multiple transformations:")
users_cleaned_strings.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 2.2 String Extraction and Manipulation
# 
# More advanced string operations include substring extraction, splitting, and concatenation.

# CELL ********************

# Demonstrate substring, split, and concatenation operations
string_operations = users_cleaned_strings.select(
    "user_id",
    col("name_trimmed").alias("full_name"),
    "email_clean",
    "city_clean",
    # Substring operations
    substring(col("name_trimmed"), 1, 5).alias("first_5_chars"),
    # Extract email username using split (more reliable than substring with instr)
    split(col("email_clean"), "@")[0].alias("email_username"),
    # Extract email domain using split
    split(col("email_clean"), "@")[1].alias("email_domain"),
    # Split operations for names
    split(col("name_trimmed"), " ").alias("name_parts"),
    split(col("name_trimmed"), " ")[0].alias("first_name"),
    # Safe extraction of last name (handle cases with only one name)
    when(size(split(col("name_trimmed"), " ")) > 1, 
         split(col("name_trimmed"), " ")[1])
    .otherwise(lit("")).alias("last_name"),
    # Concatenation operations
    concat(col("name_trimmed"), lit(" - "), col("city_clean")).alias("name_city"),
    concat_ws(", ", col("name_trimmed"), col("email_clean"), col("city_clean")).alias("contact_info"),
    # String position functions (for information only)
    instr(col("email_clean"), "@").alias("at_position"),
    locate(".", col("email_clean")).alias("dot_position")
)

print("String extraction and manipulation examples:")
#string_operations.show(truncate=False)
display(string_operations)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 3: Conditional Logic and Data Validation
# 
# Conditional logic is essential for data transformations, categorizations, and handling edge cases in data processing.

# MARKDOWN ********************

# ### 3.1 When-Otherwise Conditional Logic
# 
# The `when()` function combined with `otherwise()` provides SQL-like CASE statements for conditional transformations.

# CELL ********************

# Demonstrate when-otherwise conditional logic
print("Original transaction data for conditional processing:")
transactions_df.select("transaction_id", "amount", "currency", "status").show()

# Apply conditional logic using when-otherwise
conditional_operations = transactions_df.select(
    "transaction_id",
    "user_id",
    "amount",
    "currency",
    "status",
    # Categorize transaction amounts
    when(col("amount") < 50, "Small")
    .when(col("amount") < 200, "Medium")
    .when(col("amount") < 500, "Large")
    .otherwise("Very Large").alias("amount_category"),
    
    # Standardize currency to USD (simplified conversion)
    when(col("currency") == "EUR", col("amount") * 1.1)
    .when(col("currency") == "GBP", col("amount") * 1.25)
    .when(col("currency") == "CAD", col("amount") * 0.75)
    .otherwise(col("amount")).alias("amount_usd"),
    
    # Create status categories
    when(col("status") == "completed", "Success")
    .when(col("status") == "pending", "Processing")
    .when(col("status").isin(["failed", "cancelled"]), "Failed")
    .otherwise("Other").alias("status_category"),
    
    # Create risk score based on multiple conditions
    when((col("amount") > 1000) & (col("status") == "pending"), "High Risk")
    .when((col("amount") > 500) | (col("currency") != "USD"), "Medium Risk")
    .when(col("amount") < 100, "Low Risk")
    .otherwise("Normal").alias("risk_score"),
    
    # Handle null user_id
    when(col("user_id").isNull(), "Anonymous")
    .otherwise(col("user_id").cast("string")).alias("user_identifier")
)

print("\nConditional transformations applied:")
conditional_operations.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 3.2 Coalesce and Null Handling
# 
# The `coalesce()` function is essential for handling null values and providing fallback logic.

# CELL ********************

# Demonstrate coalesce and null handling strategies
print("Data with null values for coalesce demonstration:")
users_df.select("user_id", "full_name", "registration_date", "phone").show(truncate=False)
#products_df.select("product_id", "product_name", "stock_status").show(truncate=False)

# Apply coalesce for null handling
null_handling = users_df.select(
    "user_id",
    "full_name",
    "registration_date",
    "email",
    "phone",
    # Handle null phone numbers
    coalesce(col("phone"), lit("No phone provided")).alias("phone_with_default"),
    
    # Handle null registration dates with multiple fallbacks
    coalesce(
        to_date(col("registration_date")),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        current_date()
    ).alias("registration_date_clean"),
    
    # Create contact preference based on available data
    when(col("phone").isNotNull() & col("email").isNotNull(), "Both")
    .when(col("phone").isNotNull(), "Phone Only")
    .when(col("email").isNotNull(), "Email Only")
    .otherwise("No Contact").alias("contact_preference"),
    
    # Safe string operations with null checks
    when(col("full_name").isNotNull(), 
         concat(lit("User: "), initcap(trim(col("full_name")))))
    .otherwise(lit("Unknown User")).alias("display_name"),
    
    # Null-safe comparisons
    col("phone").isNull().alias("missing_phone"),
    col("registration_date").isNull().alias("missing_reg_date")
)

print("\nNull handling with coalesce and conditional logic:")
#null_handling.show(truncate=False)
display(null_handling)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Part 4: Common Pitfalls and Best Practices
# 
# Understanding common pitfalls and applying best practices is crucial for robust data processing pipelines.

# MARKDOWN ********************

# ### 4.1 Common Pitfalls to Avoid
# 
# Let's explore common mistakes and how to avoid them when working with data formatting operations.

# CELL ********************

# Demonstrate common pitfalls and their solutions
print("=== PITFALL 1: Assuming consistent date formats ===")

# Wrong approach - using single format
wrong_dates = users_df.select(
    "user_id",
    "registration_date",
    to_date(col("registration_date")).alias("parsed_date_wrong")
)
print("Wrong approach (single format):")
wrong_dates.show(truncate=False)

# Correct approach - using coalesce with multiple formats
correct_dates = users_df.select(
    "user_id",
    "registration_date",
    coalesce(
        to_date(col("registration_date")),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        to_date(col("registration_date"), "yyyy.MM.dd"),
        to_date(col("registration_date"), "yyyyMMdd")
    ).alias("parsed_date_correct")
)
print("\nCorrect approach (multiple formats with coalesce):")
correct_dates.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("\n=== PITFALL 2: Not handling null values properly ===")

# Wrong approach - operations on potentially null columns
print("Wrong approach (no null checks):")
wrong_nulls = users_df.select(
    "user_id",
    "phone",
    # This could produce unexpected results
    upper(col("phone")).alias("phone_upper_wrong"),
    length(col("phone")).alias("phone_length_wrong")
)
wrong_nulls.show(truncate=False)

# Correct approach - handling nulls explicitly
print("\nCorrect approach (with null handling):")
correct_nulls = users_df.select(
    "user_id",
    "phone",
    when(col("phone").isNotNull(), upper(col("phone")))
    .otherwise(lit("NO PHONE")).alias("phone_upper_correct"),
    
    coalesce(length(col("phone")), lit(0)).alias("phone_length_correct"),
    
    when(col("phone").isNotNull() & (length(col("phone")) > 0), 
         regexp_replace(col("phone"), r"[^0-9]", ""))
    .otherwise(lit("N/A")).alias("phone_digits_only")
)
correct_nulls.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### 4.2 Performance Best Practices
# 
# Optimizing data formatting operations for better performance and maintainability.

# CELL ********************

# Best practices for performance optimization
print("Chain operations efficiently")

# Create a comprehensive data cleaning pipeline
comprehensive_cleaning = users_df.select(
    "user_id",
    "full_name",
    "registration_date",
    "email",
    "city",
    "phone"
).withColumn(
    # Clean name first
    "name_clean", initcap(trim(col("full_name")))
).withColumn(
    # Split name into parts
    "name_parts", split(col("name_clean"), " ")
).withColumn(
    "first_name", col("name_parts")[0]
).withColumn(
    "last_name", 
    when(size(col("name_parts")) > 1, col("name_parts")[1])
    .otherwise(lit(""))
).withColumn(
    # Clean email
    "email_clean", lower(trim(col("email")))
).withColumn(
    # Parse registration date
    "registration_date_clean",
    coalesce(
        to_date(col("registration_date")),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        to_date(col("registration_date"), "yyyy.MM.dd"),
        to_date(col("registration_date"), "yyyyMMdd")
    )
).withColumn(
    # Clean phone
    "phone_clean", 
    when(col("phone").isNotNull(),
         regexp_replace(col("phone"), r"[^0-9]", ""))
    .otherwise(lit(None))
).withColumn(
    # Create data quality score
    "data_quality_score",
    (when(col("name_clean").isNotNull() & (length(col("name_clean")) > 0), 1).otherwise(0) +
     when(col("email_clean").isNotNull() & col("email_clean").contains("@"), 1).otherwise(0) +
     when(col("registration_date_clean").isNotNull(), 1).otherwise(0) +
     when(col("phone_clean").isNotNull() & (length(col("phone_clean")) >= 10), 1).otherwise(0))
).select(
    "user_id",
    "first_name",
    "last_name",
    "email_clean",
    "registration_date_clean",
    "phone_clean",
    "data_quality_score"
)

print("Cleaning pipeline result:")
comprehensive_cleaning.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Practical Exercises
# 
# Now it's time to practice what you've learned! Complete these exercises to reinforce your understanding of data formatting operations.

# MARKDOWN ********************

# ### Exercise 1: Date and Time Processing
# 
# Using the transaction data, create a comprehensive date/time analysis:
# 
# 1. **Parse all timestamp formats and convert to standard timestamp**
# 2. **Convert all timestamps to UTC timezone**
# 3. **Extract date components and create time-based categories - Morning, Afternoon, Evening or Night. Weekends.**
# 4. **Calculate time differences and business metrics - days since transaction.**

# CELL ********************

# Exercise 1: Complete the date/time processing pipeline
# Your code here:

exercise_1_solution = transactions_df.select(
    "transaction_id",
    "user_id",
    "transaction_time",
    "amount",
    "currency",
    "status",
    "timezone_info"
).withColumn(
    # Parse timestamp with multiple format support
    "parsed_timestamp",
    coalesce(
        to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("transaction_time"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(col("transaction_time"), "dd/MM/yyyy HH:mm"),
        to_timestamp(col("transaction_time"), "yyyy.MM.dd HH:mm:ss")
    )
).withColumn(
    # Convert to UTC (assuming parsed time is in session timezone)
    "timestamp_utc",
    col("parsed_timestamp")  # Already in UTC for this exercise
).withColumn(
    # Extract date components
    "transaction_date", to_date(col("timestamp_utc"))
).withColumn(
    "transaction_hour", hour(col("timestamp_utc"))
).withColumn(
    "transaction_day_of_week", dayofweek(col("timestamp_utc"))
).withColumn(
    # Create time-based categories
    "time_category",
    when(col("transaction_hour").between(6, 11), "Morning")
    .when(col("transaction_hour").between(12, 17), "Afternoon")
    .when(col("transaction_hour").between(18, 22), "Evening")
    .otherwise("Night")
).withColumn(
    "day_type",
    when(col("transaction_day_of_week").isin([1, 7]), "Weekend")
    .otherwise("Weekday")
).withColumn(
    # Calculate days since transaction
    "days_since_transaction",
    datediff(current_date(), col("transaction_date"))
)

print("Exercise 1 - Date/Time Processing:")
exercise_1_solution.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 2: Conditional Logic
# 
# Create a comprehensive data quality and business logic pipeline using users_df cleaned first:
# 
# 1. **Implement multi-level conditional logic for user segmentation**
# 2. **Create business rules using when-otherwise chains**
# 3. **Handle edge cases and null values properly**
# 4. **Generate summary statistics and quality metrics**
# 
# #### Rules to Apply:
# 
# **Tenure - based on days_since_registration**: 
# - New (<30)
# - Recent (30-179)
# - Established (180-364)
# - Veteran (>=365)
# 
# **Completness Score** (0-5):
# - +1: Valid Name (length > 0)
# - +1: Valid Email (contém "@") 
# - +1: Valid City (length > 0)
# - +1: Valid Date (not null)
# - +1: Valid Phone (>= 10 dígitos)
# 
# **Quality Tiers**:
# - Premium: score = 5
# - Standard: score = 3 ou 4  
# - Basic: score = 2
# - Poor: score = 0 ou 1
# 
# **Marketing Segments**:
# - VIP: Veteran + Premium
# - Loyal: (Established OR Veteran) + (Premium OR Standard)
# - Welcome: New
# - Data_Enrichment_Needed: Poor quality
# 
# **Achieve**:
# - 0 nulls in marketing_segment
# - Scores must be between 0-5
# - Users with score 5 = Premium tier


# CELL ********************

# Exercise 3: Implement complex conditional logic
# Your code here:

# First, create a cleaned user dataset
users_for_segmentation = users_df.select(
    "user_id",
    initcap(trim(col("full_name"))).alias("full_name"),
    lower(trim(col("email"))).alias("email"),
    initcap(trim(col("city"))).alias("city"),
    coalesce(
        to_date(col("registration_date")),
        to_date(col("registration_date"), "MM/dd/yyyy"),
        to_date(col("registration_date"), "yyyy.MM.dd"),
        to_date(col("registration_date"), "yyyyMMdd")
    ).alias("registration_date"),
    when(col("phone").isNotNull(),
         regexp_replace(trim(col("phone")), r"[^0-9]", "")).alias("phone_clean")
)

# Apply complex conditional logic
exercise_2_solution = users_for_segmentation.withColumn(
    # Calculate days since registration
    "days_since_registration",
    when(col("registration_date").isNotNull(),
         datediff(current_date(), col("registration_date")))
    .otherwise(lit(0))
).withColumn(
    # User tenure category
    "tenure_category",
    when(col("days_since_registration") < 30, "New")
    .when(col("days_since_registration") < 180, "Recent")
    .when(col("days_since_registration") < 365, "Established")
    .when(col("days_since_registration") >= 365, "Veteran")
    .otherwise("Unknown")
).withColumn(
    # Data completeness score
    "completeness_score",
    (when(col("full_name").isNotNull() & (length(col("full_name")) > 0), 1).otherwise(0) +
     when(col("email").isNotNull() & col("email").contains("@"), 1).otherwise(0) +
     when(col("city").isNotNull() & (length(col("city")) > 0), 1).otherwise(0) +
     when(col("registration_date").isNotNull(), 1).otherwise(0) +
     when(col("phone_clean").isNotNull() & (length(col("phone_clean")) >= 10), 1).otherwise(0))
).withColumn(
    # Data quality tier
    "quality_tier",
    when(col("completeness_score") == 5, "Premium")
    .when(col("completeness_score") >= 3, "Standard")
    .when(col("completeness_score") >= 2, "Basic")
    .otherwise("Poor")
).withColumn(
    # Geographic region (simplified)
    "region",
    when(col("city").isin(["New York", "Philadelphia"]), "Northeast")
    .when(col("city").isin(["Los Angeles"]), "West Coast")
    .when(col("city").isin(["Chicago"]), "Midwest")
    .when(col("city").isin(["Houston", "Phoenix"]), "South/Southwest")
    .otherwise("Other")
).withColumn(
    # Marketing segment
    "marketing_segment",
    when((col("tenure_category") == "Veteran") & (col("quality_tier") == "Premium"), "VIP")
    .when((col("tenure_category").isin(["Established", "Veteran"])) & 
          (col("quality_tier").isin(["Premium", "Standard"])), "Loyal")
    .when(col("tenure_category") == "New", "Welcome")
    .when(col("quality_tier") == "Poor", "Data_Enrichment_Needed")
    .otherwise("Standard")
).withColumn(
    # Risk flags
    "risk_flags",
    concat_ws(", ",
        when(col("email").isNull() | ~col("email").contains("@"), "Invalid_Email").otherwise(lit("")),
        when(col("registration_date").isNull(), "Missing_Registration").otherwise(lit("")),
        when((col("phone_clean").isNull()) | (length(col("phone_clean")) < 10), "Invalid_Phone").otherwise(lit(""))
    )
)

print("Exercise 3 - Conditional Logic:")
exercise_2_solution.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Exercise 3: Performance Analysis
# Use the data result from the exercice 2.  
# 
# Create a summary analysis of your data formatting operations:
# 
# 1. **Generate data quality metrics**
# 2. **Create performance benchmarks** 
# 3. **Identify optimization opportunities**
# 
# #### Consider these measures:
# 
# - % users in each quality tier
# - Average completeness score
# - Count missing in each field
# 
# - User count by segment
# - Average tenure by segment  
# - Average quality score by segment
# 
# - Count by region
# - Quality score by region
# - Top cities by region
# 
# - Count users by flag numbers (0,1,2,3)
# - % users "risk-free"

# CELL ********************

# Exercise 3: Performance and Quality Analysis
# Your code here:

# Data quality summary
quality_summary = exercise_2_solution.agg(
    count("*").alias("total_users"),
    countDistinct("user_id").alias("unique_users"),
    avg("completeness_score").alias("avg_completeness"),
    sum(when(col("quality_tier") == "Premium", 1).otherwise(0)).alias("premium_users"),
    sum(when(col("quality_tier") == "Poor", 1).otherwise(0)).alias("poor_quality_users"),
    sum(when(col("registration_date").isNull(), 1).otherwise(0)).alias("missing_reg_dates"),
    sum(when(col("phone_clean").isNull(), 1).otherwise(0)).alias("missing_phones")
)

print("Data Quality Summary:")
quality_summary.show()

# Segmentation distribution
segmentation_summary = exercise_2_solution.groupBy("marketing_segment", "quality_tier").agg(
    count("*").alias("user_count"),
    avg("completeness_score").alias("avg_completeness"),
    avg("days_since_registration").alias("avg_tenure_days")
).orderBy("marketing_segment", "quality_tier")

print("\nSegmentation Summary:")
segmentation_summary.show()

# Regional analysis
regional_summary = exercise_2_solution.groupBy("region").agg(
    count("*").alias("user_count"),
    countDistinct("city").alias("unique_cities"),
    avg("completeness_score").alias("avg_completeness"),
    collect_list("city").alias("cities")
).orderBy(desc("user_count"))

print("\nRegional Analysis:")
regional_summary.show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Summary and Key Takeaways
# 
# Congratulations! You've completed the Data Formatting: Dates, Strings and Conditionals lesson. Let's summarize what we've covered:
# 
# ### **Date and Timestamp Operations Mastered**:
# - **to_date() and to_timestamp()** - Converting strings to temporal types with custom formats
# - **Timezone handling** - UTC conversions and timezone-aware operations
# - **Date manipulation** - date_trunc, date_format, add_months, datediff for calculations
# - **Date arithmetic** - Computing differences, adding/subtracting periods
# 
# ### **String Processing Techniques**:
# - **Basic cleaning** - trim, lower, upper, initcap for standardization
# - **String manipulation** - substring, split, concat, concat_ws for data extraction
# - **String validation** - length, contains, rlike for data quality checks
# 
# ### **Conditional Logic Patterns**:
# - **when-otherwise chains** - Multi-level conditional transformations
# - **coalesce() function** - Null handling and fallback logic
# - **Safe casting** - Preventing runtime errors with validation
# - **Complex business rules** - Combining conditions for sophisticated logic
# 
# ### **Critical Best Practices**:
# 1. **Always handle multiple date formats** using coalesce with various patterns
# 2. **Validate data before casting** to prevent runtime errors
# 3. **Use null-safe operations** with proper when-otherwise logic
# 4. **Optimize regex patterns** by extracting once and transforming multiple times
# 5. **Chain operations efficiently** to minimize data shuffling
# 6. **Create data quality metrics** to monitor transformation success
# 
# ### **Common Pitfalls Avoided**:
# - Assuming consistent data formats without validation
# - Not handling null values properly in transformations
# - Ignoring timezone considerations in timestamp processing
# - Not validating results of casting operations

