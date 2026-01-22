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

# MARKDOWN ********************

# ## Question 3: Difference between Clustered and Non-Clustered Indexes in SQL
# 
# ### Short Answer
# - **Clustered Index**: Defines the **physical order** of data in a table; only **one per table** (usually the primary key).  
# - **Non-Clustered Index**: Separate structure pointing to data; a table can have **multiple non-clustered indexes** to speed up queries.
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **clustered** and **non-clustered indexes** improve query performance by allowing faster data retrieval. The main difference is **physical vs logical ordering** of data and the number of indexes allowed per table.
# 
# ---
# 
# ### 1. Clustered Index
# - Determines the **physical order of data rows** in the table.  
# - Each table can have **only one clustered index**.  
# - Rows are stored in the order of the **clustered index key**.  
# - Retrieval using a clustered index is **faster** when querying the indexed column.  
# - Creating or rebuilding a clustered index can be **time-consuming**, as it affects **physical data order**.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY,
#     Name VARCHAR(50),
#     Department VARCHAR(50)
# );
# ```
# -- EmployeeID becomes the clustered index automatically
# 
# ### 2. Non-Clustered Index
# - A separate data structure containing indexed columns and pointers to actual data rows.
# - A table can have multiple non-clustered indexes.
# - Does not affect physical data order; provides a quick lookup path.
# - Faster to create or rebuild compared to clustered indexes.
# 
# **Example:**  
# ```sql
# CREATE NONCLUSTERED INDEX idx_Department
# ON Employees(Department);
# ```
# ### Interview Tip
# - Clustered Index → affects physical storage, only one per table.
# - Non-Clustered Index → logical structure, allows multiple indexes, used to speed up queries.
# 
# ### Summary
# A **clustered index** determines the **physical order of data** in a table and can only be created on **one column**, while a **non-clustered index** is a **separate data structure** that allows for **multiple indexing strategies** and does **not affect the physical order of data**. The choice between a clustered and non-clustered index depends on the **specific database design** and the **types of queries** that need optimization.


# CELL ********************

## Clustered Index
# spark.sql("""
# CREATE CLUSTERED INDEX idx_emp_id ON emp_dept(emp_id)
# """).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

## NONClustered Index
# spark.sql("""
# CREATE NONCLUSTERED INDEX idx_emp_id ON emp_dept(salary)
# """).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES;").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 4: Write an SQL query to find the second highest salary of an employee without using TOP or LIMIT?

# CELL ********************

spark.sql("""
SELECT * FROM emp_dept;
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using CTE 
# NOTE: we can use both dense_rank or row_number as salary doesn't contains duplicates
# use dense_rank. This works even for duplicate salary  
spark.sql("""
WITH salary_rank_table
AS
(
    SELECT 
        *,
        DENSE_RANK() OVER(ORDER BY salary DESC) AS salary_rank
        -- ROW_NUMBER() OVER(ORDER BY salary DESC) AS salary_rank
    FROM emp_dept
)
SELECT * FROM salary_rank_table
WHERE salary_rank = 2
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Using correlated queries
spark.sql("""
SELECT * FROM emp_dept
WHERE salary < (SELECT MAX(salary) FROM emp_dept)
ORDER BY salary DESC
LIMIT 1;
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 5: How to find duplicate rows in the database?

# CELL ********************

data = [
    (1, "John",  "HR", 50000),
    (2, "Jane",  "IT", 55000),
    (3, "Mary",  "HR", 60000),
    (4, "Peter", "IT", 52000),
    (5, "Alice", "HR", 50000),
    (6, "Tom",   "IT", 52000),
    (7, "Emma",  "HR", 60000)
]

columns = ["emp_id", "emp_name", "department", "salary"]

df = spark.createDataFrame(data, columns)
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("emp_dept")

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT salary, department, COUNT(*) AS no_of_records
# MAGIC FROM emp_dept
# MAGIC GROUP BY salary,department
# MAGIC HAVING COUNT(*) > 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question: Difference between Correlated and Non-Correlated Subqueries in SQL
# 
# ### Short Answer
# - **Non-Correlated Subquery**: Executes **independently** of the outer query and runs **only once**.
# - **Correlated Subquery**: Depends on the **outer query** and is executed **once per row** of the outer query.
# - Non-correlated subqueries are generally **more efficient** than correlated subqueries.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL, a **subquery** is a query nested inside another query. Subqueries are mainly classified into **non-correlated** and **correlated** subqueries based on their dependency on the outer query.
# 
# ---
# 
# ### 1. Non-Correlated Subquery
# - Executes **independently** without referencing the outer query.  
# - Evaluated **once**, and the result is passed to the outer query.  
# - Commonly used with `IN`, `NOT IN`, or comparison operators.  
# - Generally **better for performance** since it is not repeatedly executed.
# 
# **Example:**  
# ```sql
# SELECT Name
# FROM Employees
# WHERE Salary > (
#     SELECT AVG(Salary)
#     FROM Employees
# );
# ```
# 
# ### 2. Correlated Subquery
# - Depends on values from the outer query.
# - Executed once for each row processed by the outer query.
# - Can be slower, especially with large datasets.
# - Often used when row-by-row comparison is required.
# 
# **Example:**  
# ```sql
# SELECT e1.Name
# FROM Employees e1
# WHERE Salary > (
#     SELECT AVG(Salary)
#     FROM Employees e2
#     WHERE e2.Department = e1.Department
# );
# ```
# ### Interview Tip:
# - **Non-correlated subquery** → runs **once**, **better performance**
# - **Correlated subquery** → runs **per row**, **flexible** but can be **slower**


# CELL ********************

# MAGIC %%sql
# MAGIC -- Non-correlated subqueries
# MAGIC SELECT * FROM emp_dept
# MAGIC WHERE salary > (SELECT AVG(salary) FROM emp_dept);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Correlated Subqueries
# MAGIC SELECT * FROM emp_dept AS e1
# MAGIC WHERE salary > (
# MAGIC     SELECT AVG(salary) FROM emp_dept AS e2
# MAGIC     WHERE e1.department = e2.department
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Using window function
# MAGIC SELECT * 
# MAGIC FROM (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         AVG(salary) OVER(PARTITION BY department) AS avg_salary_dept
# MAGIC     FROM emp_dept
# MAGIC ) AS t
# MAGIC WHERE salary > avg_salary_dept

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 7: How many Clustered Indexes can you have in a table?
# 
# ### Short Answer
# - You can have **only one clustered index per table**.
# 
# ---
# 
# ### Detailed Explanation
# 
# In most **relational database management systems (RDBMS)**, a table can have **only one clustered index**.  
# The clustered index determines the **physical order of data rows** in the table, and since a table can be physically organized in only **one way**, only one clustered index is allowed.
# 
# - A table can have **multiple non-clustered indexes**.  
# - Non-clustered indexes are **separate data structures** that provide **quick access paths** to data rows.  
# - Having multiple clustered indexes would require storing the same data in **multiple physical orders**, which is **inefficient and impractical**.  
# - The single clustered index restriction ensures **data consistency** and **storage efficiency**.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY,  -- Clustered index on EmployeeID
#     Name VARCHAR(50),
#     Department VARCHAR(50)
# );
# -- Only one clustered index is allowed (on EmployeeID)
# ```
# ### Interview Tip:
# - **Clustered index** → only **one per table**, **defines physical data order**.
# - **Non-clustered index** → can have **multiple**, used to **optimize queries on different columns**.


# MARKDOWN ********************

# ## Question 8: Difference between PRIMARY Key and UNIQUE Key Constraint in SQL
# 
# ### Short Answer
# - **PRIMARY Key**: Uniquely identifies each row; **cannot be NULL**; only **one per table**; usually creates a **clustered index**.  
# - **UNIQUE Key**: Ensures column values are unique; can be **NULL**; a table can have **multiple unique keys**; usually creates a **non-clustered index**.  
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **PRIMARY** and **UNIQUE** key constraints enforce **uniqueness** in a table, but they differ in purpose, limitations, and indexing.
# 
# ---
# 
# ### 1. PRIMARY Key Constraint
# - Uniquely identifies **each row** in a table.  
# - Values must be **unique** and **not NULL**.  
# - **Only one primary key** allowed per table.  
# - Typically creates a **clustered index**, defining the **physical order** of data.  
# - Must usually be defined when the table is **created**.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY,  -- PRIMARY key, cannot be NULL
#     Name VARCHAR(50),
#     Department VARCHAR(50)
# );
# ```
# 
# ### 2. UNIQUE Key Constraint
# - Ensures column values are unique but does not necessarily identify rows uniquely.
# - A table can have multiple unique keys.
# - NULL values are allowed (except in composite unique keys where all columns must be unique and not NULL).
# - Usually creates a non-clustered index to optimize queries.
# - Can be defined at table creation or added later.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY,
#     Email VARCHAR(100) UNIQUE,  -- UNIQUE key allows one NULL
#     Name VARCHAR(50)
# );
# ```
# 
# ### Interview Tip
# - PRIMARY key → one per table, uniquely identifies rows, cannot be NULL.
# - UNIQUE key → multiple per table, ensures uniqueness, can allow NULLs.
# - Use PRIMARY key for row identity, UNIQUE key for additional uniqueness constraints


# MARKDOWN ********************

# ## Question 9: Difference between View and Materialized View in SQL
# 
# ### Short Answer
# - **View**: Virtual table, does **not store data**, always shows **up-to-date** results.  
# - **Materialized View**: Physical copy of query results, **stores data**, can improve **performance**, may be **slightly stale**.  
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **views** and **materialized views** provide a **logical representation of data** from one or more underlying tables, but they differ in **storage and performance**.
# 
# ---
# 
# ### 1. View
# - A **virtual table** defined by a query.  
# - **Does not store data**; data is generated on-the-fly when queried.  
# - Simplifies **complex queries** and provides **security** by restricting access to specific columns or rows.  
# - Always shows **current data** from underlying tables.  
# - Suitable for scenarios where **real-time data** is required.
# 
# **Example:**  
# ```sql
# CREATE VIEW EmployeeView AS
# SELECT Name, Department
# FROM Employees
# WHERE Salary > 50000;
# ```
# ### 2\. Materialized View
# 
# *   A **physical copy of the query result** stored in a table.
#     
# *   Data is **precomputed and stored**, refreshed **periodically or on-demand**.
#     
# *   Improves **query performance** for **complex or resource-intensive queries**.
#     
# *   Data may **not always be up-to-date**; depends on the **last refresh**.
#     
# *   Suitable when **near-real-time data** is sufficient.
#     
# 
# **Example:**  
# ```sql
#  CREATE MATERIALIZED VIEW EmployeeSummary AS 
#  SELECT Department, COUNT(*) AS EmployeeCount, AVG(Salary) AS AvgSalary  
#  FROM Employees  
#  GROUP BY Department  
#  WITH REFRESH FAST ON DEMAND;
# ```
# ### Interview Tip
# 
# *   **View** → virtual table, always **up-to-date**, hides complexity.
#     
# *   **Materialized View** → physical table, **precomputed results**, faster queries, may have **slightly stale data**.
#     
# *   Use **views** for **real-time access** and **materialized views** for **performance optimization**.


# MARKDOWN ********************

# ## Question 10: Difference between TRUNCATE, DELETE, and DROP in SQL
# 
# ### Short Answer
# - **TRUNCATE**: Deletes **all rows** from a table, **keeps table structure**, faster, cannot be rolled back.  
# - **DELETE**: Deletes **specific rows** based on conditions, slower, **can be rolled back**.  
# - **DROP**: Deletes **entire database objects** (table, view, index), **cannot be rolled back**, removes structure and data.
# 
# ---
# 
# ### Detailed Explanation
# 
# **TRUNCATE**, **DELETE**, and **DROP** are used to remove data or database objects in SQL, but they differ in purpose, logging, and rollback capability.
# 
# ---
# 
# ### 1. TRUNCATE
# - Deletes **all rows** from a table while keeping the **table structure intact**.  
# - **DML command**; faster than DELETE because it does **not log individual row deletions** or generate much undo/redo data.  
# - Cannot be used on tables with **foreign key constraints**.  
# - Cannot be **rolled back**.  
# - Resets **identity counters** (if any) and empties the table.
# 
# **Example:**  
# ```sql
# TRUNCATE TABLE employees;
# ```
# 
# ### 2\. DELETE
# 
# *   Deletes **specific rows** based on a **WHERE clause**.
#     
# *   **DML command**; slower than TRUNCATE because it **logs each row deletion**, generates undo/redo data, and checks constraints/triggers.
#     
# *   Can be **rolled back** using ROLLBACK.
#     
# 
# **Example:**
# 
# ```sql
# DELETE FROM employees  WHERE department = 'HR';
# ```
# 
# ### 3\. DROP
# 
# *   Deletes **database objects** like tables, views, indexes, or stored procedures.
#     
# *   **DDL command**; completely removes the object and its data from the database.
#     
# *   Cannot be **rolled back**; deletion is permanent.
#     
# *   Use with caution to avoid **data loss**.
#     
# 
# **Example:**
# 
# ```sql
#  DROP TABLE employees;
# ```
# 
# ### Interview Tip
# 
# *   **TRUNCATE** → fast, removes all rows, keeps table structure, cannot roll back.
#     
# *   **DELETE** → conditional row deletion, slower, can roll back.
#     
# *   **DROP** → removes table/object completely, permanent, use carefully.


# MARKDOWN ********************

# ## Question 11: What is Referential Integrity in a Relational Database?
# 
# ### Short Answer
# - Referential Integrity is a rule that ensures when a record is deleted from the **primary table**, all associated records in the **related table** are handled according to defined rules.  
# - It ensures **data consistency and integrity** across tables.
# 
# ---
# 
# ### Detailed Explanation
# 
# Referential Integrity (RI) is a concept in relational databases that ensures the **validity and consistency** of relationships between tables. It prevents **orphaned or invalid data** by enforcing rules on how foreign key values relate to primary key values.
# 
# ---
# 
# ### 1. Primary Key - Foreign Key Relationship
# - The **Primary Key** column(s) uniquely identify rows in a table.  
# - The **Foreign Key** column(s) in another table reference the Primary Key column(s).  
# - Ensures each Foreign Key value in the referencing table has a corresponding value in the referenced table.
# 
# **Example:**  
# ```sql
# CREATE TABLE Departments (
#     DepartmentID INT PRIMARY KEY,
#     DepartmentName VARCHAR(50)
# );
# 
# CREATE TABLE Employees (
#     EmployeeID INT PRIMARY KEY,
#     Name VARCHAR(50),
#     DepartmentID INT,
#     FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID)
# );
# ```
# 
# ### 2\. Maintaining Data Integrity
# 
# *   Prevents insertion of **invalid foreign key values** in the referencing table.
#     
# *   Ensures **relationships between tables remain consistent**.
#     
# *   Avoids **orphaned rows** in the related table.
#     
# 
# ### 3\. Enforcing Constraints
# 
# *   **Foreign Key constraints** enforce Referential Integrity rules.
#     
# *   Can be **declarative** (explicitly defined) or **implicit** (created automatically by the database).
#     
# 
# ### 4\. Cascading Actions
# 
# *   Referential Integrity can include **cascading actions** to handle updates or deletions in the referenced table:
#     
#     *   CASCADE → update/delete referencing rows automatically.
#         
#     *   SET NULL → set referencing rows to NULL.
#         
#     *   RESTRICT → prevent update/delete if referenced rows exist.
#         
# 
# **Example of CASCADE Delete:**
# ```sql
# CREATE TABLE Employees (EmployeeID INT PRIMARY KEY,
#     Name VARCHAR(50),
#     DepartmentID INT,
#     FOREIGN KEY (DepartmentID) REFERENCES Departments(DepartmentID) ON DELETE CASCADE);   
# ```
# ### Interview Tip
# 
# *   **Referential Integrity** ensures **data relationships are consistent and reliable**.
#     
# *   Use **Foreign Key constraints** to enforce RI.
#     
# *   Understand **cascading actions** (CASCADE, SET NULL, RESTRICT) — common interview question.


# MARKDOWN ********************

# ## Question 12: What is Normalization in SQL?
# 
# ### Short Answer
# - Normalization is a technique used to **avoid data duplication** and **improve data integrity** by organizing data into multiple related tables.
# 
# ---
# 
# ### Detailed Explanation
# 
# Normalization is the process of organizing and structuring a **relational database** to reduce **data redundancy** and improve **data integrity**. It involves dividing large tables into smaller, well-structured tables and applying **normal forms** to ensure that each table stores data about a single subject.
# 
# The main goals of normalization are:
# - Eliminate **data redundancy**
# - Prevent **insert, update, and delete anomalies**
# - Ensure **data consistency and integrity**
# 
# ---
# 
# ### Unnormalized Table Example
# 
# | Student_ID | Student_Name | Course_Code | Course_Name | Instructor | Instructor_Email |
# |------------|--------------|-------------|-------------|------------|------------------|
# | 101 | John | CS101 | Intro to CS | Prof. Smith | smith@example.com |
# | 101 | John | MATH101 | Math Basics | Prof. Johnson | johnson@example.com |
# | 102 | Alice | CS101 | Intro to CS | Prof. Smith | smith@example.com |
# | 103 | Bob | MATH101 | Math Basics | Prof. Johnson | johnson@example.com |
# 
# This table contains **redundant data**, leading to potential inconsistencies.
# 
# ---
# 
# ### Normalization Process
# 
# ### 1. First Normal Form (1NF)
# - Ensures each column contains **atomic (single) values**
# - Removes repeating groups
# 
# **Students Table:**  
# | Student_ID | Student_Name |
# |------------|--------------|
# | 101 | John |
# | 102 | Alice |
# | 103 | Bob |
# 
# **Courses Table:**  
# | Course_Code | Course_Name | Instructor | Instructor_Email |
# |-------------|-------------|------------|------------------|
# | CS101 | Intro to CS | Prof. Smith | smith@example.com |
# | MATH101 | Math Basics | Prof. Johnson | johnson@example.com |
# 
# ---
# 
# ### 2. Second Normal Form (2NF)
# - Ensures all **non-key attributes** depend on the **entire primary key**
# - The Courses table already satisfies 2NF
# 
# ---
# 
# ### 3. Third Normal Form (3NF)
# - Removes **transitive dependencies**
# - Non-key attributes must depend **only on the primary key**
# 
# **Courses Table:**  
# | Course_Code | Course_Name | Instructor_Code |
# |-------------|-------------|-----------------|
# | CS101 | Intro to CS | 1 |
# | MATH101 | Math Basics | 2 |
# 
# **Instructors Table:**  
# | Instructor_Code | Instructor | Instructor_Email |
# |-----------------|------------|------------------|
# | 1 | Prof. Smith | smith@example.com |
# | 2 | Prof. Johnson | johnson@example.com |
# 
# ---
# 
# ### Higher Normal Forms
# 
# ### 4. Boyce-Codd Normal Form (BCNF)
# - Every determinant is a **candidate key**
# - Students table already satisfies BCNF
# 
# ### 5. Fourth Normal Form (4NF)
# - No **multi-valued dependencies**
# - Database satisfies 4NF
# 
# ### 6. Fifth Normal Form (5NF)
# - No **join dependency anomalies**
# - Database satisfies 5NF
# 
# ---
# 
# ### Final Normalized Tables
# 
# **Students Table:**  
# | Student_ID | Student_Name |
# |------------|--------------|
# | 101 | John |
# | 102 | Alice |
# | 103 | Bob |
# 
# **Courses Table:**  
# | Course_Code | Course_Name | Instructor_Code |
# |-------------|-------------|-----------------|
# | CS101 | Intro to CS | 1 |
# | MATH101 | Math Basics | 2 |
# 
# **Instructors Table:**  
# | Instructor_Code | Instructor | Instructor_Email |
# |-----------------|------------|------------------|
# | 1 | Prof. Smith | smith@example.com |
# | 2 | Prof. Johnson | johnson@example.com |
# 
# ---
# 
# ### Interview Tip
# - **Normalization** reduces redundancy and prevents anomalies  
# - **3NF** is usually sufficient for most applications  
# - Use normalization for **OLTP systems**, denormalization for **OLAP/reporting**


# CELL ********************

data = [
    (1, "John", "IT", ["P1", "P2"], ["ERP", "CRM"], "Alice"),
    (2, "Mary", "HR", ["P3"], ["Recruitment"], "Bob"),
    (3, "John", "IT", ["P2"], ["CRM"], "Alice")
]

columns = [
    "Emp_ID",
    "Emp_Name",
    "Dept",
    "Project_IDs",
    "Project_Names",
    "Manager"
]

df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").saveAsTable("unnormalize")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM unnormalize;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode,arrays_zip,col

df_1nf = df.withColumn("Project",explode(arrays_zip("Project_IDs","Project_Names")))\
.select("Emp_ID","Emp_Name","Dept",col("Project.Project_IDs").alias("Project_ID"),col("Project.Project_Names").alias("Project_Name"),"Manager")
df_1nf.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp = df_1nf.select("Emp_ID","Emp_Name","Dept","Manager").distinct()
df_emp.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_project = df_1nf.select("Project_ID","Project_Name").distinct()
df_project.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

emp_project_df = df_1nf.select("Emp_ID","Project_ID").distinct()
emp_project_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dept_df = df_emp.select("Dept","Manager").distinct()
dept_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_emp = df_emp.select("Emp_ID","Emp_Name","Dept")
df_emp.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 13: When is a table said to be in 1NF, 2NF, and 3NF?
# 
# ### Short Answer
# - **1NF**: Table has **atomic values**, no repeating groups or arrays.  
# - **2NF**: Table is in 1NF, and all non-key columns are **fully dependent on the primary key**.  
# - **3NF**: Table is in 2NF, and there are **no transitive dependencies** (non-key columns do not depend on other non-key columns).
# 
# ---
# 
# ### Detailed Explanation
# 
# Normalization is the process of structuring a database to **reduce redundancy** and **improve data integrity**. Each normal form builds upon the previous one, providing increasing levels of data organization.
# 
# ---
# 
# ### 1. First Normal Form (1NF)
# - Each column contains **atomic (single) values**.  
# - No column contains arrays or **repeating groups**.  
# - Each row is **unique**.
# 
# **Example:**  
# | Emp_ID | Emp_Name | Projects |
# |--------|---------|---------|
# | 1 | John | [P1, P2] |  ← Violates 1NF  
# 
# **After 1NF (explode multi-valued columns):**  
# | Emp_ID | Emp_Name | Project |
# |--------|---------|---------|
# | 1 | John | P1 |
# | 1 | John | P2 |
# 
# ---
# 
# ### 2. Second Normal Form (2NF)
# - Must already satisfy **1NF**.  
# - All **non-key columns** must be **fully dependent** on the **entire primary key**.  
# - Eliminates **partial dependencies**.
# 
# **Example:**  
# | Emp_ID | Project | Emp_Name | Dept |  ← Emp_Name depends only on Emp_ID, not (Emp_ID, Project)  
# 
# **Split into 2NF tables:**  
# - Employees(Emp_ID, Emp_Name, Dept)  
# - Employee_Project(Emp_ID, Project)
# 
# ---
# 
# ### 3. Third Normal Form (3NF)
# - Must already satisfy **2NF**.  
# - No **transitive dependencies**: non-key columns should not depend on other non-key columns.  
# 
# **Example:**  
# | Emp_ID | Emp_Name | Dept | Manager |  ← Manager depends on Dept (non-key), not Emp_ID  
# 
# **Split into 3NF tables:**  
# - Employees(Emp_ID, Emp_Name, Dept)  
# - Departments(Dept, Manager)
# 
# ---
# 
# ### Interview Tip
# - **1NF** → atomic values, no arrays/lists  
# - **2NF** → remove partial dependencies  
# - **3NF** → remove transitive dependencies  
# - Normalize at least **up to 3NF** to prevent anomalies and ensure consistent data management


# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
