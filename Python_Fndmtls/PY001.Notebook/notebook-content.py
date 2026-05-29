# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# MARKDOWN ********************

# ## PY001 🟢 Python Fundamentals (Part 1)
# 
# > **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute.
# 
# This "Python Fundamentals" mini-series of tutorials is different to anything else you will find, in that: 
# 1. I will teach what you need to know, specifically for building data solutions in Fabric - this is not just a generic "Learn Python" series. 
# 2. I'm a realist - as of 2026, LLMs have changed how we write code, and what part humans will play in the process. I've written this series with that in mind. I'll frequently make reference to this, and we'll be covering how to leverage AI in a responsible way, and how to review AI-generated code.   
# 
# The series begins with two notebooks that teach the absolute fundamentals, these are the rules of the language, and the most common operations & syntax that you just need to know, if you want to write, or review Python in real-world data projects. 
# 
# All of the techniques / syntax in these two notebooks I use to develop Fabric solutions, and I'll be referencing specific use cases / real-world projects from other parts of the Dojo to show you that EVERYTHING here is essential (and used). 
# 
# #### Prerequisites
# 1. Download this notebook from the Skool community 
# 2. In a Fabric Workspace, click 'Import' > 'Notebook' > 'From this Computer'. 
# 3. Open the notebook and run it! 
# 
# 
# #### A very quick note on what you're looking at
# This is a Fabric Notebook. 
# - Notebooks are a convenient way of structuring, running and documenting code. 
# - There are pros and cons to using them but for most users, I recommend you write your code in Notebooks - they are very user friendly, and they are great for teaching, so I use them quite a lot in my communities! 
# 
# In Fabric, we have Notebooks which can be written to connect to different engines (runtimes) Python, PySpark, Spark SQL, T-SQL are probably the most common ones you'll need to know. 
# 
# We'll discuss the others in more detail, later in this module. For now, just know that this is a Python notebook (because we are focusing just on Python to begin with). 
# 
# #### Running a cell and testing your connection
# The `print("")` function is used all the time - it simply prints something to the output pane, which in a notebook is just below each cell. Between the brackets, you pass in the message you want to print using the speechmarks to represent a String - for example "Hello there".   
# 
# To execute a cell, there are a few ways: 
# - Click "Execute/ Run individual cells"
# - From within the cell itself - hit Shft + Enter


# CELL ********************

print("Hey there, welcome to PY001!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We also have variables in Python. So we can define a variable like this: 

# CELL ********************

my_name = "Will"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Rather than just printing a static string (like above), we can make it dynamic, using a concept known as an f-string. In Python, we can put an f"" in front of a string, and it will allow us to inject variables into your string, to make it more dynamic.  

# CELL ********************

print(f"Hey there {my_name}, welcome to PY001!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# This seems trivial, but the reality is, f-strings are one of my most used Python bits of syntax. In Fabric you can use this for: 
# - writing custom logging - which writes out the specific error message (not a generic one) 
# - writing dynamic Spark SQL statements in a Notebook 
# - constructing ABFS paths to write to Lakehouses in different Workspaces
# - and much more, of course

# MARKDOWN ********************

# #### Base data types
# Let's show you some of the main data types that exist in Python:

# CELL ********************

# str - text
store_name = "Dojo Retail - Manchester"
region = "North West"

# int - whole numbers
units_sold_today = 1842
staff_on_shift = 8

# float - decimal numbers
average_price_gbp = 12.49
vat_rate = 0.20

# bool - True or False
is_open = True
accepts_returns = True

# None - absence of a value (the equivalent of NULL in SQL)
last_stocktake_date = None

print(store_name)
print(units_sold_today)
print(average_price_gbp)
print(is_open)
print(last_stocktake_date)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# You can check the type of any object in Python, using `type(variable)`: 

# CELL ********************

type(is_open)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Something really important to note about Python is that it's an 'interpreted' language. This means you don't have to declare the type of an object when you define it. For example, above, when we ran: `units_sold_today = 1842` this will only really get tested when your code runs. 
# 
# Python interprets the type based on the value - which has it's pros and cons. It means Python syntax is very concise, but for enterprise data engineering scenarios, this can lead to type errors. 
# 
# Other programming languages, like TypeScript for example, are 'statically-typed' languages which means you need to be very clear on the type of an object when you define it upfront, and it checks every time you declare a variable / object of that type!
# 
# For now, this is just something you need to be concious of - we'll look at how we can add more type-robustness to our Python code later in the series. 
# 
# #### Arithmetic operations
# 


# CELL ********************

# Multiplication
gross_revenue = units_sold_today * average_price_gbp
print(gross_revenue)

# Division
revenue_per_staff = gross_revenue / staff_on_shift
print(revenue_per_staff)

# Integer division (rounds down to whole number)
units_per_staff = units_sold_today // staff_on_shift
print(units_per_staff)

# Modulo (remainder after division)
leftover_units = units_sold_today % staff_on_shift
print(leftover_units)

# VAT calculation
vat_amount = gross_revenue * vat_rate
net_revenue = gross_revenue - vat_amount
print(net_revenue)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# ## Lists & dicts
# These two really are the bread and butter of data engineering - you will use these in nearly all Fabric data engineering use cases, so it's important to really master them. 
# 
# Let's start with **Lists**: 
# 
# #### Lists 
# 
# If you imagine a database table (or spreadsheet) with 5 rows - this can be represented with a Python list (or multiple lists, which we'll look at below). 
# 
# You can define a list in Python using the square brackets, for example, we might have a list which represents a column of data in our database, or our spreadsheet, like this:  

# CELL ********************

# for example
employee_ids = [0, 1, 2, 3, 4] 
employee_names = ["Alice", "Bob", "Charlie", "David", "Eve"]
employee_salaries = [30000.00, 32000.00, 35000.00, 28000.00, 40000.00]


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# This way of representing data is quite common, especially as a lot of the data types and tools we use in Fabric are 'columnar' - meaning they work by storing data in columns (rather than rows) - so things like the Delta format, and also the Spark engine - you'll see data represented like this a lot. 
# 
# #### Doing useful things with lists

# CELL ********************

# checking membership of a value in a list with the `in` keyword
print("Alice" in employee_names)  # True
print("Savannah" in employee_names)  # False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# ordering a list
ordered_salaries = sorted(employee_salaries, reverse=True)
print(ordered_salaries)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# getting the top n salaries
top_3_salaries = ordered_salaries[:3]
print(top_3_salaries)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

# get bottom salary
bottom_salary = ordered_salaries[-1]
print(bottom_salary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We won't go through every possible combination of accessing values in a list, but these are some of the main ones. Just know that it's possible to access values in a list in pretty much every way you can think of: top n, bottom n, first, last, ranges in the middle too, you don't need to memorize the syntax (AI can do that for you), but just know what's possible. 
# 
# #### Looping through a list
# Regularly, we want to iterate through a list, and do something with every item. 
# 
# There are two main ways to do that: the for loop (good for beginners), and in most professional scenarios, you'll see this as a List Comprehension - let's look at both: 


# CELL ********************

# for each value in the employee_names list, do SOMETHING...
# (in this case, print the name in uppercase)
# upper is a string method that converts string values to UPPERCASE

for employee in employee_names: 
    print(employee.upper()) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# This list comprehension has a slightly different synax, but it does a similar thing *see slight caveat below. 
# 
# The generic structure looks like this: [_action_ for _item_ in _list_]

# CELL ********************

# the same thing, but using a list comprehension
employees_upper = [employee.upper() for employee in employee_names]
print(employees_upper)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# The slight difference is that with a list comprehension, we normally use it to create a new list, from the original list, hence why we're _assigning_ the output to `employees_upper`. 

# CELL ********************

# checking the type of the new list
type(employees_upper)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Dictionaries
# A dict is another Python data type that you will use ALL the time. It's used to stored key:value pairs. For a dictionary, we use the curly brackets, like this: `{"key": "value"}`  
# 
# In the data world, you might see a row of data stored as a dictionary (for example, if it comes from a REST API). 
# 
# Here's what the first row in our example above would look like, represented as a Python dictionary: 


# CELL ********************

first_employee = {
    "id": 0,
    "name": "Alice",
    "salary": 30000.00
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We can access specific values of a dictionary, using `dict.get(key)`: 
#  

# CELL ********************

first_employee.get("name")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We can check to see if a key exists in a dictionary using the `in` keyword 

# CELL ********************

print("id" in first_employee)  # True
print("home_office" in first_employee)  # False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# You can access the keys of a dictionary, and the values of a dictionary separately: 

# CELL ********************

print( first_employee.keys() )
print( first_employee.values() )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We can use a for-loop combined with dict.items() to loop through all the keys and values in a dictionary, like this: 

# CELL ********************

for key, value in first_employee.items():
    print(f"{key}: {value}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# You should note, here I've used the words 'key' and 'value', but you can actually define it using any words you like (although I wouldn't recommend banana & potato 😂):

# CELL ********************

# you will frequently see it shortened to just k and v 
# as long as when you reference the same thing inside your for loop, it will work

for k, v in first_employee.items():
    print(f"{k}: {v}")

for banana, potato in first_employee.items():
    print(f"{banana}: {potato}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# And we can also combine this syntax, with the list comprehension too!

# CELL ********************

# print the keys in a dictionary using a list comprehension
[print(k) for k,v in first_employee.items()] 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Lists of dictionaries
# 
# Lists are very important, and dictionaries are also very important. In the data world, lists of dictionaries are something you will see ALL the time. A list, where all the values in the list are a dictionary. 
# 
# In our original dataset, which we defined as three lists, these lines represented 5 rows in a database / spreadsheet: 
# 
# ```
# employee_ids = [0, 1, 2, 3, 4] 
# employee_names = ["Alice", "Bob", "Charlie", "David", "Eve"]
# employee_salaries = [30000.00, 32000.00, 35000.00, 28000.00, 40000.00]
# ```
# 
# Another way of representing the same dataset would be to have a single list, but have each row represented by a single dictionary. This is what that would look like:  

# CELL ********************

employee_data = [
    {"id": 0, "name": "Alice", "salary": 30000.00},
    {"id": 1, "name": "Bob", "salary": 32000.00},
    {"id": 2, "name": "Charlie", "salary": 35000.00},
    {"id": 3, "name": "David", "salary": 28000.00},
    {"id": 4, "name": "Eve", "salary": 40000.00}
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# This is exactly the same data, just represented as a list of dictionaries, rather than three separate lists. 
# 
# > Note: if you've done the Pipeline tutorials on data structures, hopefully this is familiar to you - but now we're learning how to represent these type of data structures, using Python, rather than the Pipeline activities - but it's conceptually exactly the same. 
# 
# And with this data structure, we can do lots of useful things like list filtering, based on the data in each dictionary, and much more! Here's what the list filtering example would look like: 

# CELL ********************

# get the employees with a salary above 30000
high_salary_employees = [employee.get("name") for employee in employee_data if employee["salary"] > 30000]
print(high_salary_employees)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# > Note: we'll go into a lot more detail on equality and greater/less than in the next tutorial, because it's an important concept to get right. 
# 
# ## Wrapping up
# Let's leave it there for this tutorial - I think we've crammed in enough new concepts for you to try and digest. 
# 
# Take some time, go back through the notebook from top to bottom again, and create your own cells, playing around with the syntax and the topics covered to ensure you fully understand how each part works. 
# 
# Then, when you're ready, head into the second Python Fundamentals notebook PY002. I'll see you there! 🙌 
