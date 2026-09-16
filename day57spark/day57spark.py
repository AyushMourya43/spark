# ============================================================
# CLASS 7A — SPARK JOINS FUNDAMENTALS
# HOMEWORK ASSIGNMENT
# ============================================================

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder 
    .appName("Class7A_Join_Homework") 
    .master("local[*]")
    .getOrCreate()
)

# ------------------------------------------------------------
# EMPLOYEES
# ------------------------------------------------------------

employees_data = [
    (101, "Rahul", 10, 85000),
    (102, "Priya", 20, 92000),
    (103, "Amit", 10, 78000),
    (104, "Neha", 30, 70000),
    (105, "Arjun", 40, 88000),
    (106, "Sneha", 50, 76000),
    (107, "Vikram", 60, 81000)
]

employees = spark.createDataFrame(
    employees_data,
    ["employee_id", "employee_name", "department_id", "salary"]
)

e = employees.alias("e")
# ------------------------------------------------------------
# DEPARTMENTS
# ------------------------------------------------------------

departments_data = [
    (10, "Engineering"),
    (20, "Data Science"),
    (30, "Human Resources"),
    (40, "Finance"),
    (50, "Marketing"),
    (70, "Legal")
]

departments = spark.createDataFrame(
    departments_data,
    ["department_id", "department_name"]
)

d = departments.alias("d")
# ------------------------------------------------------------
# PROJECTS
# ------------------------------------------------------------

projects_data = [
    (501, "Cloud Migration", 101),
    (502, "Customer Analytics", 102),
    (503, "Payment Gateway", 103),
    (504, "HR Automation", 104),
    (505, "Financial Dashboard", 105),
    (506, "Marketing Campaign", 108)
]

projects = spark.createDataFrame(
    projects_data,
    ["project_id", "project_name", "employee_id"]
)

p = projects.alias("p")
# ------------------------------------------------------------
# OFFICES
# ------------------------------------------------------------

offices_data = [
    (10, "Bangalore"),
    (20, "Mumbai"),
    (30, "Delhi"),
    (40, "Hyderabad"),
    (50, "Pune"),
    (80, "Chennai")
]

offices = spark.createDataFrame(
    offices_data,
    ["department_id", "office_city"]
)

# Display the DataFrames

employees.show()
departments.show()
projects.show()
offices.show()

# Q1. EMPLOYEE-DEPARTMENT INNER JOIN

# Perform an INNER JOIN between employees and departments
# using department_id.

# Display:

# employee_id
# employee_name
# department_name
# salary

# Question:
# Which employees have a matching department?
# rahul and amit

employees.join(departments, employees.department_id == departments.department_id, "inner") .select(
    employees.employee_id,
    employees.employee_name,
    departments.department_name,
    employees.salary
).show()


# Q2. ALL EMPLOYEES WITH DEPARTMENT INFORMATION

# Perform a LEFT JOIN between employees and departments.

# Display:

# employee_name
# department_id
# department_name

# Make sure every employee is present in the result.

# Question:
# What happens to the department_name for an employee whose
# department does not exist in the departments DataFrame?
# IT WILL SHOW NULL

employees.join(departments, employees.department_id == departments.department_id, "left") .select(
    employees.employee_name,
    employees.department_id,
    departments.department_name
).show()

# Q3. ALL DEPARTMENTS

# Perform a RIGHT JOIN between employees and departments.

# Display:

# employee_id
# employee_name
# department_id
# department_name

# Make sure every department is present.

# Question:
# Which department does not currently have a matching employee?
# LEGAL

employees.join(departments, employees.department_id == departments.department_id, "right") .select(
    employees.employee_id,
    employees.employee_name,
    departments.department_id,
    departments.department_name
).show()

# Q4. FULL OUTER JOIN — EMPLOYEES AND DEPARTMENTS

# Perform a FULL OUTER JOIN between employees and departments
# using department_id.

# Display:

# employee_name
# department_name
# department_id

# Identify:

# 1. Employees whose department does not exist.
# 2. Departments that do not have a matching employee.

employees.join(departments, employees.department_id == departments.department_id, "full") .select(
    employees.employee_name,
    departments.department_name,
    departments.department_id
).show()

# Q5. ALIASING

# Create aliases for employees and departments:

# e = employees.alias("e")
# d = departments.alias("d")

# Perform an INNER JOIN using the aliases.

# Display:

# e.employee_id
# e.employee_name
# d.department_name
# e.salary

# Requirement:
# Use aliases when referencing the columns.

e.join(d, e.department_id == d.department_id, "inner") .select(
    e.employee_id,
    e.employee_name,
    d.department_name,
    e.salary
).show()

# Q6. DUPLICATE COLUMN HANDLING

# Join employees and departments using department_id.

# Create a clean DataFrame containing ONLY:

# employee_id
# employee_name
# department_name
# salary

# The final DataFrame should NOT contain two department_id
# columns.

# Hint:
# Use aliases and select only the required columns.

e.join(d, e.department_id == d.department_id, "inner") .select(
    e.employee_id,
    e.employee_name,
    d.department_name,
    e.salary
).show()

# Q7. MULTIPLE JOINS

# Use the following relationship:

# employees
#      ↓
#   projects

# Join the employees and projects DataFrames using employee_id.

# Display:

# employee_name
# project_id
# project_name

# Question:
# Which employees are assigned to projects?

e.join(p, e.employee_id == p.employee_id, "inner") .select(
    e.employee_name,
    p.project_id,
    p.project_name
).show()

# Q8. THREE-TABLE JOIN

# Create a report using:

# employees
#      ↓
# departments
#      ↓
# projects

# Use appropriate JOIN conditions.

# Display:

# employee_name
# department_name
# project_name
# salary

# Use aliases to make the join conditions clear.

ed = e.join(d,e.department_id == d.department_id,"inner")

ed = ed.alias("ed")

result = ed.join(p,ed.employee_id == p.employee_id,"inner").select(
    ed.employee_name,
    ed.department_name,
    p.project_name,
    ed.salary
)

input("Press Enter to stop Spark...")