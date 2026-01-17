# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6c5bb61-be2f-46c7-bbeb-0881d1321199",
# META       "default_lakehouse_name": "practice_lakehouse",
# META       "default_lakehouse_workspace_id": "ef36d848-2f1e-494a-82b4-2edd9dbfc107",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6c5bb61-be2f-46c7-bbeb-0881d1321199"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # SQL and Database Telephonic Interview Questions:

# MARKDOWN ********************

# ## SQL Interview Preparation – Quick Revision Notes
# 
# Preparing for SQL interviews requires a combination of **theoretical knowledge** and **practical experience**.
# 
# ### Key Preparation Tips
# 
# - **Review SQL Basics**
#   - Understand SQL fundamentals such as **data types, operators, and syntax**.
# 
# - **Practice SQL Queries**
#   - Write and execute queries involving:
#     - `WHERE` (filtering)
#     - `ORDER BY` (sorting)
#     - `GROUP BY` (aggregation)
#     - `JOIN` (combining multiple tables)
# 
# - **Familiarize with DBMS**
#   - Get hands-on experience with popular systems like:
#     - **MySQL**
#     - **Oracle**
#     - **SQL Server**
# 
# - **Learn Database Normalization**
#   - Understand principles of normalization to **reduce redundancy** and **improve data integrity**.
# 
# - **Understand Data Modeling**
#   - Learn how to design **conceptual data models** before creating database schemas.
# 
# - **Brush Up on Statistics & Data Analysis**
#   - Be comfortable with **basic statistics and analytical concepts**, as many SQL interviews test these skills.
# 
# - **Practice SQL Interview Questions**
#   - Solve **sample interview questions** from online platforms to improve speed and confidence.


# MARKDOWN ********************

# ## SQL & Database Topics to Prepare for Interviews
# 
# The following are **must-know SQL and database topics** for technical interviews.  
# Use this list as a **pre-interview revision checklist**.
# 
# ### Core Topics
# 
# - **SQL and Database Basics**
#   - Tables, schemas, keys (Primary, Foreign), constraints, data types
# 
# - **SQL JOINs**
#   - `INNER`, `LEFT`, `RIGHT`, `FULL`
#   - Self join and cross join (conceptual understanding)
# 
# - **SQL Queries from Interviews**
#   - Real-world query patterns
#   - Subqueries, CTEs, case-based questions
# 
# - **Indexes**
#   - Clustered vs Non-clustered
#   - When to use indexes and performance impact
# 
# - **GROUP BY**
#   - Grouping data
#   - Using `HAVING` vs `WHERE`
# 
# - **Aggregate Functions**
#   - `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
#   - Using aggregates with `GROUP BY`
# 
# - **Stored Procedures**
#   - Creating and executing procedures
#   - Parameters and basic control flow
# 
# - **Triggers and Views**
#   - When and why to use triggers
#   - Creating and querying views
# 
# - **Advanced SQL Questions**
#   - Window functions
#   - CTEs
#   - Query optimization
#   - Complex joins and nested queries


# MARKDOWN ********************

# # Questions:

# MARKDOWN ********************

# ## Question 1: Difference between UNION and UNION ALL in SQL
# 
# ### Short Answer
# - **UNION** removes duplicate records.
# - **UNION ALL** includes duplicate records.
# - Both are used to combine results from multiple `SELECT` queries.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL, both **UNION** and **UNION ALL** are used to combine the result sets of two or more `SELECT` statements into a single result set.  
# The main difference lies in **duplicate handling and performance**.
# 
# ---
# 
# ### 1. UNION
# - Combines results from multiple `SELECT` queries.
# - **Removes duplicate rows** from the final result set.
# - Performs an **implicit DISTINCT** operation.
# - Useful when you want **unique records only**.
# 
# **Note:** Duplicate elimination makes `UNION` **slower** compared to `UNION ALL`.
# 
# ---
# 
# ### 2. UNION ALL
# - Combines results from multiple `SELECT` queries.
# - **Includes all rows**, even duplicates.
# - Does **not** perform duplicate elimination.
# - Faster than `UNION`, especially for **large datasets**.
# - Useful when **duplicates are acceptable or required**.
# 
# ---
# 
# ### Interview Tip
# - Use **UNION** when data uniqueness is important.
# - Use **UNION ALL** when performance matters and duplicates are allowed.


# CELL ********************

employees_data = [
    (1, "John", 50000),
    (2, "Jane", 55000),
    (3, "Mary", 60000),
    (4, "Peter", 52000)
]

contractors_data = [
    (101, "Sam", 48000),
    (102, "Kate", 51000),
    (103, "Alice", 48000),
    (104, "Mike", 52000)
]

emp_df = spark.createDataFrame(employees_data,["emp_id","emp_name","emp_salary"])
contractors_df = spark.createDataFrame(contractors_data,["contractor_id", "contractor_name", "contractor_salary"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_df.write.format("delta").mode("overwrite").saveAsTable("employees")
contractors_df.write.format("delta").mode("overwrite").saveAsTable("contractors")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(emp_df)
display(contractors_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
SHOW TABLES;
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using UNION
spark.sql("""
SELECT emp_id, emp_name, emp_salary FROM employees
UNION
SELECT contractor_id, contractor_name, contractor_salary FROM contractors
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using UNION ALL
spark.sql("""
SELECT emp_id, emp_name, emp_salary FROM employees
UNION ALL
SELECT contractor_id, contractor_name, contractor_salary FROM contractors
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 2: Difference between WHERE and HAVING Clause in SQL
# 
# ### Short Answer
# - **WHERE**: Filters rows **before aggregation**.
# - **HAVING**: Filters rows **after aggregation**.
# - `WHERE` works on individual rows, `HAVING` works on grouped data.
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **WHERE** and **HAVING** are used to filter data in SQL, but at **different stages** of query processing.
# 
# ---
# 
# ### 1. WHERE Clause
# - Filters rows **before any grouping or aggregation**.
# - Operates on **individual table rows** based on specified conditions.
# - Typically used with **non-aggregate functions**.
# - Helps reduce the number of rows considered for aggregation.
#   
# **Example Use:**  
# ```sql
# SELECT * FROM Employees
# WHERE Salary > 50000;
# ```
# 
# ### 2. HAVING Clause
# 
# - Filters the result of a grouped query (**after GROUP BY**).  
# - Operates on **groups of rows**.  
# - Typically used with **aggregate functions** like `COUNT`, `SUM`, `AVG`.  
# - Allows filtering based on **aggregated values**.
# 
# **Example:**  
# ```sql
# SELECT Department, COUNT(*) AS EmployeeCount
# FROM Employees
# GROUP BY Department
# HAVING COUNT(*) > 10;
# ```
# ### Interview Tip
# - WHERE → before GROUP BY
# - HAVING → after GROUP BY


# CELL ********************

employees_dept_data = [
    (1, "John","HR", 50000),
    (2, "Jane","IT", 55000),
    (3, "Mary","HR", 60000),
    (4, "Peter","IT", 52000)
]

emp_dept_df = spark.createDataFrame(employees_dept_data,["emp_id","emp_name","department","salary"])

emp_dept_df.write.format("delta").mode("overwrite").saveAsTable("emp_dept")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(emp_dept_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using WHERE clause
# Suppose we want to retrieve the employees who belong to the HR
# department and have a salary greater than 55000:
spark.sql("""
SELECT * FROM emp_dept
WHERE department = "HR" AND salary > 55000;
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using HAVING clause
# Suppose we want to retrieve the departments with an average salary greater than 51000:
spark.sql("""
SELECT department, AVG(salary) AS avg_dept_salary FROM emp_dept
GROUP BY department
HAVING AVG(salary) > 51000 
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
