# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.12"
# META   }
# META }

# MARKDOWN ********************

# ## PY002 🟢 Python Fundamentals for Fabric (Part 2)
# 
# > **Note**: this tutorial is provided for educational purposes, for members of the [Fabric Dojo community](https://skool.com/fabricdojo/about). All content contained within is protected by Copyright © law. Do not copy or re-distribute.
# 
# Welcome to Part 2 in our Python Fundamentals for Fabric mini-series. 
# 
# In Part 1, we covered the first round of fundamentals - variables, data types, lists, dictionaries and looping. In this part, we'll build on that and cover some more, really important syntax and rules of the language. Learn these and you'll be well on your way to writing Python-based Fabric solutions in no time!
# 
# Here's what we'll cover: 
# 1. Control flow - equality vs assignment, conditionals (`if`), `while` loops, and `try: except`
# 2. Functions - the building blocks of any reusable Python code
# 
# 
# #### Prerequisites
# 1. You've worked through PY001 - this notebook builds directly on that one
# 2. Download this notebook from the Skool community
# 3. In a Fabric Workspace, click 'Import' > 'Notebook' > 'From this Computer'. 
# 4. Open the notebook and run it! 


# MARKDOWN ********************

# ## Part 1: Control Flow 
# One of the core use cases for Microsoft Fabric is _orchestration_ of data processing tasks - in fact we have Pipelines (and Apache Airflow jobs) specifically for that. But these Fabric items are really just a fancy UI for managing control flow.  
# 
# With Python, we can achieve the same thing, managing what happens, when, in our code, under certain conditions. In this notebook, we'll look at all the different ways to manage this. 
# 
# #### Equality (==) vs assignment (=) 
# 
# - A single `=` is **assignment** - we use it to _assign_ a value to a variable. "I want to give this variable, this value". 
# - A double `==` is **equality** - we use it to check whether two values are equal. It returns a boolean (True or False). "I want to check if this thing is the same as this thing, return a boolean". 
# 
# These are completely different things, but they look very similar - so be careful!  
# 
# We use the equality operator == A LOT to manage control flow (that's why I'm mentioning it here). 
# 
# Let's look at it in action, with another new concept the `if` statement which is key for managing how our code runs: 


# CELL ********************

# assignment - we are SETTING the value of units_sold_today to 1842
units_sold_today = 1842

# equality operator to check a data type, 
# and taking different actions depending on the result
if type(units_sold_today) == int: 
    print("It's an int") 
elif type(units_sold_today) == float:
    print("It's a float")
elif type(units_sold_today) == list:
    print("It's a list")
else: 
    print("It's not an int") 


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Now that we can ask True/False questions, we can use those to control which code runs. This is what the `if` statement is for.
# 
# The structure is: 
# - `if` _condition_ : do something
# - `elif` _another condition_ : (optional) if the first `if` statement conditional check returns False, then it will move downways to an `elif` (you can have multiple elif statements). 
# - `else` : (optional) the `else statement means: do this if none of the above were true 
# 
# The indentation is required, not optional. 
# 
# As well as the equality (==) operator, there's more to be aware of, all of these return a boolean (True or False), and so can be used within the if statement: 
# 
#  - `==` equal to                  
#   - `!=` not equal to            
#   - `>` greater than            
#   - `<` less than
#   - `>=` greater than or equal to
#   - `<=` less than or equal to

# CELL ********************

units_sold_today = 1842

if units_sold_today > 2000:
    print("Great day - over 2000 units sold!")
elif units_sold_today > 1000:
    print("Decent day - over 1000 units sold.")
else:
    print("Quiet day in the store today.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# You'll see this type of syntax in a lot of Fabric notebooks, for example: 
# - Checking whether a Lakehouse table exists before trying to read from it/ write to it
# - Checking if a key exists in a JSON object you just got from a REST API. 
# - Skipping a row of data if a key field is `None`
# - Checking if the notebook is running in DEV or PRODUCTION. If it's in DEV you might want to use more verbose logging to debug an issue. 
# 
# #### Combining conditions with and, or, not
# We can add multiple conditions too, if you want to check more than one thing, and combine them using `and`, `or` and `not` for this

# CELL ********************

is_open = True
staff_on_shift = 8

# both conditions must be true
if is_open and staff_on_shift >= 5:
    print("Store is open and adequately staffed")

# at least one condition must be true 
if staff_on_shift > 10 or units_sold_today > 1500:
    print("Either we're well staffed, or we've had a busy day")

# not flips a boolean
if not is_open:
    print("Store is closed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Whilst we're on this topic, something I see quite a lot, but is often misunderstood, is using a value itself as the conditional for an if statement. Let me explain with code: 

# CELL ********************

error_message = None

if error_message: 
    print("There has been an error")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

error_message = ""

if error_message: 
    print("There has been an error")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# CELL ********************

error_message = "ERROR 204: Out of stock"

if error_message: 
    print("There has been an error")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# When you use a value as the conditional check of the if statement (without a comparison operator), Python evaluates it for _truthiness_ - which is not quite the same as asking "does this variable have a value?". 
# 
# The values that Python treats as _falsy_ (i.e. if statement will skip the block) are: 
# - `None`
# - `False`
# - `0` and `0.0`
# - An empty string `""`
# - An empty list `[]`, dict `{}` or tuple `()`
# 
# Everything else is _truthy_. 
# 
# That's why we saw the following: 
# - `error_message = None` → falsy → block does not run. ✅ correct
# - `error_message = ""` → falsy → block does not run. ⚠️ but maybe there _was_ an error, just with no message attached! This is a very common misunderstanding of how thsee conditional 
# - `error_message = "ERROR 204..."` → truthy → block runs ✅
# 
# The fix is to be always be explicit about what you're actually checking for, especially for values that you don't control (i.e. data values from external systems). For example: 

# CELL ********************

error_message = ""

# explicit None check - only treats None as "no error"
if error_message is not None:
    print("The first check detected an error")

# if you want to explicitly check for an empty string as well
if error_message is not None and error_message != "":
    print("The second check has detected an error")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### while loops
# In PY001, we covered for loops - which iterate over a collection (a list, a dict etc.). 
# 
# A while loop is similar, but a bit different - it just keeps running, as long as your conditional statement returns True. 
# 
# 
# A very common use case for while statements is execution a Fabric pipeline programmatically, and then 'polling' the execution status of the pipeline, until it finished. So in plain language, this would be "run my [Pipeline](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job?tabs=HTTP), and then WHILE the pipeline status is not "Succeeded" keep checking the [status](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/get-item-job-instance?tabs=HTTP) of the pipeline (perhaps every minute), once the status becomes "Succeeded", then continue with our code, to refresh the semantic model (for example). 
# 
# Other common use cases:
# - Paginating through a REST API until there are no more pages
# - Retrying a failed operation until it succeeds (or you hit a max retry count)
# 
# You do have to be careful though which value you should for the conditional statement, otherwise you might end up in an infinite loop, which will not be kind on your Fabric capacity 😀 (for example if your condition never actually becomes False, and that's what it's checking for). 


# CELL ********************

# a simple example - simulating polling a job until it's done
attempts = 0
max_attempts = 5
job_status = "running"

while job_status == "running" and attempts < max_attempts:
    attempts += 1   # this is shorthand for: attempts = attempts + 1
    print(f"Checking job status... attempt {attempts}")
    if attempts == 3:
        job_status = "succeeded"

print(f"Final status: {job_status}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Try & except blocks for handling errors
# Try / except is a really important bit of Python syntax. It allows us to take control when errors arise in your code. It's a key tool in defensive programming (which is a core part in data engineering). 
# 
# Ideally, we want to be proactive with error-handling (foresee when they might appear), and then guard against them. This is better than reacting to notebook errors that block pipelines from running successfully. 
# 
# That's what try / except is for. 

# CELL ********************

# without try/except - this would just crash and stop everything
employee_data = [
    {"id": 0, "name": "Alice", "salary": 30000.00},
    {"id": 1, "name": "Bob", "salary": "35000.00"},  # string instead of float - will cause an error
    {"id": 2, "name": "Charlie", "salary": 35000.00}
]

for employee in employee_data: 
    try:
        annual_bonus = employee["salary"] * 0.10
        print(f"{employee['name']}: bonus of {annual_bonus}")
    except TypeError as e:
        # this runs if the code in the try block throws a TypeError
        print(f"Could not calculate bonus for {employee['name']} - reason: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Notice a few things in the example above: 
# - We caught a specific error type (`TypeError`). You can also use a bare `except:` to catch _any_ error, but that's generally bad practice - you want to know what kind of error happened. (But it's better than nothing to be honest!).  
# - We captured the error itself in the variable `e`, and used it inside an f-string to write a useful log message. This is exactly the kind of custom logging I mentioned in PY001. 
# - The loop kept going - Bob's bad data didn't stop us from processing Charlie. 
# 
# A few more handle bits of syntax to know with try/except blocks: 
# - you can also chain multiple `except` blocks, to handle different error types differently -
# - you can add a `finally` block which runs regardless of whether an error was thrown. 
# - you can use `raise` keyword to actually throw an error, and stop the execution of your python (or Notebook). 

# MARKDOWN ********************

# ## Part 2: Functions
# Functions are something I use all the time in Fabric, you need to understand how they work in detail. 
# 
# They are fairly simply to understand the basics, but have layers of understanding.  
# 
# A function is simply a packaged-up bit of logic. We use functions when we want to run the same bit of logic multiple times in our codebase. 
# 
# You might've heard of the "DRY (Don't Repeat Yourself)" principle in programming. In general, we never want to write code in two places that does the same thing, so instead we write functions, and call the functions in different parts of your codebase / notebook. 
# 
# I'm going to write a number of different functions, and the same functions written in different ways to explain the main concepts in Python function definition that you will likely encouter. 

# CELL ********************

# basic function definition 
def calculate_bonus(salary):
    bonus = salary * 0.10
    return bonus

# when we define a function, it doesn't actually run the code inside 
# we need to explicitly call the function to execute its code

employee_bonus = calculate_bonus(30000)

# we assign the return value from the function to a variable employee_bonus 
# so that we can use it later in our code 

print(f"Calculated bonus: {employee_bonus}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Our functions can have MULTIPLE outputs 

# CELL ********************

def calculate_bonus(salary):
    bonus = salary * 0.10
    full_compensation = salary + bonus
    return bonus, full_compensation 

# above, here we are returning two values from the function as a tuple


# 'unpacking' the two return values into separate variables 
# note they don't have to be called the same thing as in the return statement of the function

bonus, total = calculate_bonus(30000)

print(f"Bonus: {bonus}, Total Compensation: {total}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We can have multiple inputs, and also define default values for the inputs: 

# CELL ********************

def calculate_bonus(salary, bonus_rate = 0.10):
    return salary * bonus_rate

# uses the default 0.10
print(f"Initial bonus: {calculate_bonus(30000)}")

# overrides the default - and notice we're naming the argument explicitly
# this is called a 'keyword argument' and it makes code much more readable
print(f"Bonus with custom rate: {calculate_bonus(salary=30000, bonus_rate=0.15)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Generally, we want to make our functions as easy to understand as possible, for that, we can add in our own documentation/ hints to help other people understand our functions (and AI perhaps). 
# 
# Note neither the docstring or type hint make any functional changes to how the code runs, they are merely for documenting.

# CELL ********************

# function definition with a type hint - 
# note: this is just for readability and doesn't actually enforce types in Python
def calculate_bonus(salary: float) -> float:
    bonus = salary * 0.10
    return bonus

print(calculate_bonus(30000))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# We can also add the """ docstring """ underneath the def line. 
# 
# You need two sets of """ (three speech marks) to define your docstring. 
# 
# This should describe what the function does concisely. Sometimes it also gives hints as to the inputs and outputs.  

# CELL ********************

def calculate_bonus(salary: float) -> float:
    """ Calculates a bonus as 10% of the salary.
    Input: salary comes from REST API
    Output: bonus 

    """
    bonus = salary * 0.10
    return bonus


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# With a docstring, any user can call help(function_name) from anywhere in your codebase to get more information about it, and in many code editors, if you hover over a function, you'll also see the docstring explanation of the code, so that's quite helpful! 

# CELL ********************

help(calculate_bonus)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# #### Putting is all together
# 
# Let's combine a few things we've covered in this tutorial. 
# 
# Here's a function that processes a list of employee dictionaries and returns the names of high earners - using a try/except, a list comprehension, an `if` filter, type hints, and a default argument. 

# CELL ********************

def get_high_earners(employees: list, threshold: float = 30000) -> list:
    try:
        return [employee["name"] for employee in employees if employee["salary"] > threshold]
    except (KeyError, TypeError) as err:
        print(f"Could not process employees - reason: {err}")
        return []

employee_data = [
    {"id": 0, "name": "Alice", "salary": 30000.00},
    {"id": 1, "name": "Bob", "salary": 32000.00},
    {"id": 2, "name": "Charlie", "salary": 35000.00},
    {"id": 3, "name": "David", "salary": 28000.00},
    {"id": 4, "name": "Eve", "salary": 40000.00}
]

print(get_high_earners(employee_data)) # uses default value
print(get_high_earners(employee_data, threshold=33000)) # overrides default

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }

# MARKDOWN ********************

# Take a moment to look at that function and try to understand each element - everything in there is something we've covered in PY001 or PY002. Hopefully it's starting to click into place! 

# MARKDOWN ********************

# ## Wrapping up
# Congratulations, you've covered a lot of ground in these first two Python Fundamentals tutorials. 
# 
# Between PY001 and PY002, you've now covered: 
# - Variables, data types, f-strings, arithmetic
# - Lists, dictionaries, list comprehensions, looping
# - Equality vs assignment, conditionals, logical operators
# - `while` loops and `try` / `except` for robust code
# - Functions with type hints, default arguments and keyword arguments
# 
# That's probably 80% of the syntax you'll need for the vast majority of Fabric Python work. The rest you will accumulate with experience, mainly around working with specific libraries (PySpark, `notebookutils`, the Fabric SDK etc.). We'll cover those later in this module of the Dojo. 
# 
# As before - go back through this notebook, create your own cells, and play around with the syntax until each piece feels natural. Try writing your own function. Try breaking it on purpose, and catching the error with `try`/`except`. The more you experiment, the faster it'll stick. 
# 
# See you in the next module! 🙌 

