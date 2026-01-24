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


# MARKDOWN ********************

# ## Question 14: Difference between ISNULL() and COALESCE() in SQL Server
# 
# ### Short Answer
# - **ISNULL()**: SQL Server–specific, returns a **replacement value** if the expression is NULL.  
# - **COALESCE()**: Standard SQL, returns the **first non-NULL value** from a list of expressions.  
# - COALESCE() is more **flexible** and **portable**.
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **ISNULL()** and **COALESCE()** are used to handle **NULL values** in SQL Server, but differ in **usage, flexibility, and portability**.
# 
# ---
# 
# ### 1. ISNULL()
# - Built-in SQL Server function.  
# - Takes **two arguments**: the expression and the replacement value.  
# - Returns the **replacement value** if the expression is NULL; otherwise returns the expression.  
# - Specific to SQL Server.
# 
# **Example:**  
# ```sql
# SELECT ISNULL(column_name, 'Not available') AS result
# FROM table_name;
# -- Result: If column_name is NULL → 'Not available', else the value of column_name.
# ```
# ### 2\. COALESCE()
# 
# -   Standard SQL function supported by many DBMS.
# 
# -   Takes **multiple arguments**.
# 
# -   Returns the **first non-NULL value** in the list.
# 
# -   If all values are NULL → returns NULL (or can provide a default value at the end).
# 
# **Example:**
# ```sql
# SELECT COALESCE(column1, column2, column3, 'Not available') AS result
# FROM table_name;`
# 
# --*Result:* Returns the first non-NULL among `column1`, `column2`, `column3`.\
# --If all are NULL → `'Not available'`.
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Feature | ISNULL() | COALESCE() |
# | --- | --- | --- |
# | Arguments | 2 | Multiple |
# | SQL Standard | SQL Server only | Standard SQL |
# | Returns | Replacement if NULL | First non-NULL value |
# | Portability | Limited | High |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Use **ISNULL()** for simple, two-value NULL handling in SQL Server.
# 
# -   Use **COALESCE()** when you need **multiple fallback values** or **cross-platform compatibility**.
# 
# -   Remember: **COALESCE() is more flexible and standard**---a good habit for interviews.


# CELL ********************

# ISNULL(expr,replacement value) only works in SQL Server
# SELECT ISNULL(NULL,"Contains null value") AS result;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- COALESCE() works in SQL SERVER and ANSI SQL
# MAGIC SELECT coalesce(NULL,NULL,"not null","Replacement") AS result;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 15: How do you ensure that only values between 1 to 5 are allowed in an integer column?
# 
# ### Short Answer
# - Use a **CHECK constraint** with a **BETWEEN clause** to enforce that only values **1 to 5** can be inserted or updated.  
# - Prevents invalid data from entering the table, ensuring **data integrity**.
# 
# ---
# 
# ### Detailed Explanation
# 
# A **CHECK constraint** allows you to define a condition that must be **true for all rows** in a table.  
# - If a value violates the constraint, the **INSERT** or **UPDATE** operation is rejected.  
# - Using `BETWEEN 1 AND 5` ensures that only integers from **1 to 5 inclusive** are allowed.
# 
# ---
# 
# ### Example: Create Table with CHECK Constraint
# 
# ```sql
# CREATE TABLE your_table (
#     your_column INT,
#     CONSTRAINT chk_value_range CHECK (your_column BETWEEN 1 AND 5)
# );
# ```
# ### Example: Insert Values
# 
# **Valid Insert:**
# 
# ```sql
# INSERT INTO your_table (your_column) VALUES (3);
# ```
# 
# -   ✅ Accepted, because 3 is between 1 and 5
# 
# **Invalid Insert:**
# 
# ```sql
# INSERT INTO your_table (your_column) VALUES (7);
# ```
# 
# -   ❌ Rejected, because 7 is outside the allowed range
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   **CHECK constraints** enforce **business rules at the database level**.
# 
# -   Use **BETWEEN** for ranges and **logical operators** (`AND`, `OR`) for complex conditions.
# 
# -   Always name constraints for **better error handling and readability**.


# MARKDOWN ********************

# ## Question 16: Difference between CHAR and VARCHAR data types in SQL
# 
# ### Short Answer
# - **CHAR**: Fixed-length character strings, padded with spaces if shorter than defined length.  
# - **VARCHAR**: Variable-length character strings, uses storage based on actual length of data.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL, both **CHAR** and **VARCHAR** store character data, but they differ in **storage and behavior**:
# 
# ---
# 
# ### 1. CHAR Data Type
# - Stands for **character**, used for **fixed-length strings**.  
# - Column length is **fixed**; shorter values are **padded with spaces**.  
# - Storage is **constant**, regardless of actual string length.  
# - Suitable for **consistent-length data**.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     Emp_Code CHAR(10)
# );
# 
# INSERT INTO Employees (Emp_Code) VALUES ('A123');
# -- Stored as 'A123      ' (padded with spaces to length 10)
# ```
# 
# ### 2\. VARCHAR Data Type
# 
# -   Stands for **variable-length character**.
# 
# -   Column length can vary up to a **maximum defined length**.
# 
# -   Only uses **storage for actual string length** (no padding).
# 
# -   Efficient for **variable-length data**.
# 
# **Example:**
# ```sql
# CREATE TABLE Employees (
#     Emp_Name VARCHAR(50)
# );
# 
# INSERT INTO Employees (Emp_Name) VALUES ('John');
# -- Stored as 'John' (length 4, no padding)`
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Feature | CHAR | VARCHAR |
# | --- | --- | --- |
# | Length | Fixed | Variable |
# | Padding | Yes | No |
# | Storage | Always occupies full length | Only uses required space |
# | Best Use | Data with consistent length | Data with variable length |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Use **CHAR** for fixed-size codes like `country_code`, `gender`.
# 
# -   Use **VARCHAR** for names, addresses, descriptions, or any variable-length data.
# 
# -   Remember: **CHAR wastes storage for short strings; VARCHAR is flexible and space-efficient.**


# MARKDOWN ********************

# ## Question 17: Difference between VARCHAR and NVARCHAR in SQL Server
# 
# ### Short Answer
# - **VARCHAR**: Stores **non-Unicode** characters (ASCII), uses less storage.  
# - **NVARCHAR**: Stores **Unicode** characters, supports **multilingual data**, uses more storage.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL Server, both **VARCHAR** and **NVARCHAR** are used to store character data, but they differ in **encoding and storage requirements**:
# 
# ---
# 
# ### 1. VARCHAR
# - Stores **non-Unicode characters** using the database's default code page.  
# - Suitable for **single-byte character sets** (English, numbers, symbols).  
# - Uses **1 byte per character**.
# 
# **Example:**  
# ```sql
# CREATE TABLE Employees (
#     Emp_Name VARCHAR(50)
# );
# 
# INSERT INTO Employees (Emp_Name) VALUES ('John');
# ```
# ### 2\. NVARCHAR
# 
# -   Stores **Unicode characters** using **UTF-16 encoding**.
# 
# -   Supports **multi-byte characters** for international languages.
# 
# -   Uses **2 bytes per character**, consuming more storage.
# 
# **Example:**
# ```sql
# CREATE TABLE Employees (
#     Emp_Name NVARCHAR(50)
# );
# 
# INSERT INTO Employees (Emp_Name) VALUES (N'张伟');  -- Unicode string`
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Feature | VARCHAR | NVARCHAR |
# | --- | --- | --- |
# | Encoding | Non-Unicode | Unicode (UTF-16) |
# | Storage | 1 byte per char | 2 bytes per char |
# | Language Support | Single-byte (English) | Multi-byte, multilingual |
# | Best Use | English-only data | International/multilingual data |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   **VARCHAR** → Efficient for English or single-byte text
# 
# -   **NVARCHAR** → Always use when **storing international/multilingual data**
# 
# -   Prefix Unicode literals with `N` when inserting into NVARCHAR columns


# MARKDOWN ********************

# ## Question 18: How do you get Day, Month, and Year from a date in SQL Server?
# 
# ### Short Answer
# - Use the built-in functions: **DAY()**, **MONTH()**, **YEAR()**  
# - Alternatively, use **DATEPART()** for more flexibility.
# 
# ---
# 
# ### Detailed Explanation
# 
# SQL Server provides multiple ways to extract components from a date column:
# 
# ---
# 
# ### 1. Get Day
# - Use the **DAY()** function to get the day of the month (1–31).  
# 
# **Example:**  
# ```sql
# SELECT DAY(your_date_column) AS Day
# FROM your_table;
# ```
# ### 2\. Get Month
# 
# -   Use the **MONTH()** function to get the month (1--12).
# 
# **Example:**
# ```sql
# SELECT MONTH(your_date_column) AS Month
# FROM your_table;`
# ```
# * * * * *
# 
# ### 3\. Get Year
# 
# -   Use the **YEAR()** function to get the year.
# 
# **Example:**
# ```sql
# SELECT YEAR(your_date_column) AS Year
# FROM your_table;`
# ```
# * * * * *
# 
# ### Using DATEPART()
# 
# -   DATEPART() provides more flexibility for different date parts.
# 
# **Example:**
# ```sql
# SELECT
#     DATEPART(day, your_date_column) AS Day,
#     DATEPART(month, your_date_column) AS Month,
#     DATEPART(year, your_date_column) AS Year
# FROM your_table;`
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   **DAY(), MONTH(), YEAR()** → simple and easy to remember.
# 
# -   **DATEPART()** → versatile, use for **weekday, quarter, hour, minute**, etc.
# 
# -   Always make sure the column is of **DATE, DATETIME, or DATETIME2 type** to avoid errors.


# MARKDOWN ********************

# ## Question 19: How to check if a date is valid in SQL?
# 
# ### Short Answer
# - Use **TRY_CAST()** or **TRY_CONVERT()** to attempt converting a string to a **DATE** type.  
# - If conversion returns **NULL**, the date is invalid; otherwise, it is valid.
# 
# ---
# 
# ### Detailed Explanation
# 
# SQL Server provides **safe functions** to check date validity:
# 
# 1. **TRY_CAST()**  
#    - Attempts to cast a value to a specified data type.  
#    - Returns **NULL** if the conversion fails.
# 
# **Example:**  
# ```sql
# DECLARE @dateString VARCHAR(20) = '2025-08-24';
# 
# IF TRY_CAST(@dateString AS DATE) IS NOT NULL
# BEGIN
#     PRINT 'Valid Date';
# END
# ELSE
# BEGIN
#     PRINT 'Invalid Date';
# END
# ```
# 1.  **TRY_CONVERT()**
# 
#     -   Similar to TRY_CAST(), attempts conversion to a target type.
# 
#     -   Returns **NULL** if invalid.
# 
# **Example:**
# ```sql
# DECLARE @dateString VARCHAR(20) = '2025-08-24';
# 
# IF TRY_CONVERT(DATE, @dateString) IS NOT NULL
# BEGIN
#     PRINT 'Valid Date';
# END
# ELSE
# BEGIN
#     PRINT 'Invalid Date';
# END
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   Use **TRY_CAST()** or **TRY_CONVERT()** when validating **user inputs** or **external data**.
# 
# -   Always check for **NULL** to safely identify invalid dates.
# 
# -   Avoid using direct CAST/CONVERT without error handling, as it will **throw an exception** for invalid dates.


# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC SELECT 
# MAGIC     CASE 
# MAGIC         WHEN TRY_CAST('2025-02-28' AS DATE) IS NOT NULL THEN 'Valid'
# MAGIC         ELSE 'Invalid'
# MAGIC     END AS result;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 20: Difference between LEFT OUTER JOIN and INNER JOIN in SQL
# 
# ### Short Answer
# - **INNER JOIN**: Returns only rows with **matching keys** in both tables.  
# - **LEFT OUTER JOIN**: Returns **all rows from the left table**, with matching rows from the right table if available; otherwise, **NULL** for right table columns.
# 
# ---
# 
# ### Detailed Explanation
# 
# Both **INNER JOIN** and **LEFT OUTER JOIN** are used to combine data from two or more tables, but the **result sets differ**.
# 
# ---
# 
# ### 1. INNER JOIN
# - Returns rows where there is a **match in both tables**.  
# - Filters out rows with no match in either table.
# 
# **Example:**  
# ```sql
# SELECT e.Emp_ID, e.Emp_Name, d.Dept_Name
# FROM Employees e
# INNER JOIN Departments d
# ON e.Department_ID = d.Department_ID;
# ```
# *Result:* Only employees who have a valid department ID are returned.
# 
# ### 2\. LEFT OUTER JOIN
# 
# -   Returns **all rows from the left table** (`Employees`).
# 
# -   Returns matching rows from the right table (`Departments`).
# 
# -   If no match exists, the right table columns return **NULL**.
# 
# **Example:**
# ```sql
# SELECT e.Emp_ID, e.Emp_Name, d.Dept_Name
# FROM Employees e
# LEFT OUTER JOIN Departments d
# ON e.Department_ID = d.Department_ID;`
# ```
# *Result:* All employees are returned.
# 
# -   Employees with a valid department → department name shown
# 
# -   Employees with no department → `NULL` in `Dept_Name` column
# 
# * * * * *
# 
# ### Key Differences
# 
# | Feature | INNER JOIN | LEFT OUTER JOIN |
# | --- | --- | --- |
# | Rows Returned | Only matching rows | All left table rows, match from right table if exists |
# | Non-Matching Rows | Excluded | Included with NULLs for right table columns |
# 
# ### Interview Tip
# 
# -   Use **INNER JOIN** to get **only related records**.
# 
# -   Use **LEFT OUTER JOIN** to get **all records from left table**, even if no match exists in right table.
# 
# -   Visualize with a **Venn diagram**: INNER = intersection, LEFT OUTER = all left + intersection.


# MARKDOWN ********************

# ## Question 21: What is SELF JOIN in SQL?
# 
# ### Short Answer
# - A **SELF JOIN** is a join in which a table is joined with **itself**.  
# - Useful for comparing rows within the same table, such as **employees and managers**.
# 
# ---
# 
# ### Detailed Explanation
# 
# A **SELF JOIN** allows you to query **relationships within the same table**.  
# - The table is **aliased** into two instances to distinguish them.  
# - Can be used with **INNER JOIN, LEFT JOIN, etc.**, depending on the requirement.
# 
# ---
# 
# ### Example: Employees and Managers
# 
# Assume an `Employees` table:
# 
# | Emp_ID | Emp_Name | Manager_ID |
# |--------|----------|------------|
# | 1      | John     | NULL       |
# | 2      | Mary     | 1          |
# | 3      | Bob      | 1          |
# 
# **Query:** Find employees and their managers:
# 
# ```sql
# SELECT e.Emp_Name AS Employee, m.Emp_Name AS Manager
# FROM Employees e
# LEFT JOIN Employees m
# ON e.Manager_ID = m.Emp_ID;
# ```
# **Result:**
# 
# | Employee | Manager |
# | --- | --- |
# | John | NULL |
# | Mary | John |
# | Bob | John |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Always use **table aliases** to distinguish the same table instances.
# 
# -   Commonly used for **hierarchical data**, like employees, categories, or organizational structures.
# 
# -   Remember: SELF JOIN is **not a special join type**, just a table joining itself.


# CELL ********************

departments_data = [
    (10, "IT"),
    (20, "HR"),
    (30, "Finance"),
    (40, "Marketing")
]

departments_cols = ["department_id", "department_name"]

df_departments = spark.createDataFrame(departments_data, departments_cols)
df_departments.write.format("delta").mode("overwrite").saveAsTable("depts")
df_departments.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

employees_data = [
    (1, "John", 10),
    (2, "Alice", 10),
    (3, "Bob", 20),
    (4, "Mary", None)   # Employee without department
]

employees_cols = ["employee_id", "employee_name", "department_id"]

df_employees = spark.createDataFrame(employees_data, employees_cols)
df_employees.write.format("delta").mode("overwrite").saveAsTable("emp")
df_employees.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 22: Print all departments and number of employees in each department
# 
# ### Short Answer
# - Use **LEFT OUTER JOIN** to include all departments.
# - Use **COUNT()** to calculate the number of employees.
# - Use **GROUP BY** to aggregate results department-wise.
# 
# ---
# 
# ### Detailed Explanation
# 
# In a classical **Employee–Department** relationship:
# - Each employee belongs to a department.
# - Some departments may have **no employees**.
# 
# To display **all departments** along with the **number of employees**, we use:
# - **LEFT OUTER JOIN** → keeps all departments
# - **COUNT(employee_id)** → counts employees per department
# - **GROUP BY** → groups records by department
# 
# ---
# 
# ### SQL Query
# 
# ```sql
# SELECT
#     d.department_name,
#     COUNT(e.employee_id) AS no_of_emps
# FROM departments d
# LEFT OUTER JOIN employees e
#     ON d.department_id = e.department_id
# GROUP BY d.department_name;
# ```
# ### How It Works
# 
# -   Start from the **departments** table.
# 
# -   LEFT OUTER JOIN ensures every department appears, even without employees.
# 
# -   `COUNT(e.employee_id)` counts only non-NULL employee IDs.
# 
# -   `GROUP BY d.department_name` groups employees under each department.
# 
# * * * * *
# 
# ### Example Output
# 
# | department_name | no_of_emps |
# | --- | --- |
# | IT | 5 |
# | HR | 2 |
# | Finance | 0 |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Always use **LEFT OUTER JOIN** when the question says **"print all departments"**.
# 
# -   `COUNT(column)` ignores NULLs → perfect for counting related records.
# 
# -   INNER JOIN ❌ will miss departments with zero employees.


# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC     d.department_name, COUNT(e.employee_id) AS no_of_emps
# MAGIC FROM depts AS d
# MAGIC LEFT JOIN emp AS e
# MAGIC     ON d.department_id = e.department_id
# MAGIC GROUP BY d.department_name

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Question 23: Difference between COUNT(*), COUNT(1), and COUNT(column_name) in SQL
# 
# ### Short Answer
# - **COUNT(*)**: Counts all rows, including rows with NULL values.
# - **COUNT(1)**: Counts all rows, same behavior as COUNT(*).
# - **COUNT(column_name)**: Counts only **non-NULL values** in the specified column.
# 
# ---
# 
# ### Detailed Explanation
# 
# The **COUNT()** aggregate function is used to count rows or values in SQL, but its behavior depends on how it is used.
# 
# ---
# 
# ### 1. COUNT(*)
# - Counts the **total number of rows** returned by the query.
# - Includes rows even if all columns contain **NULL values**.
# - Does not evaluate individual column values.
# 
# **Example:**  
# ```sql
# SELECT COUNT(*) AS total_rows
# FROM employees;
# ```
# ### 2\. COUNT(1)
# 
# -   Counts the **total number of rows**, same as COUNT(*).
# 
# -   Uses a constant value (`1`) for each row.
# 
# -   Performance and behavior are **identical to COUNT(*)** in modern SQL engines.
# 
# **Example:**
# ```sql
# SELECT COUNT(1) AS total_rows
# FROM employees;
# ```
# * * * * *
# 
# ### 3\. COUNT(column_name)
# 
# -   Counts only **non-NULL values** in the specified column.
# 
# -   Rows where the column value is NULL are **excluded**.
# 
# **Example:**
# ```sql
# SELECT COUNT(employee_id) AS non_null_employee_ids
# FROM employees;
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Function | What It Counts | NULL Handling |
# | --- | --- | --- |
# | COUNT(*) | All rows | NULLs included |
# | COUNT(1) | All rows | NULLs included |
# | COUNT(column_name) | Non-NULL column values | NULLs excluded |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   **COUNT(*) and COUNT(1) behave the same** --- choose COUNT(*) for clarity.
# 
# -   Use **COUNT(column_name)** when you need to count actual values.
# 
# -   Never say COUNT(1) ignores NULLs --- that's a **common interview trap**.


# MARKDOWN ********************

# ## Question 24: What is Database Statistics and how does it affect query performance?
# 
# ### Short Answer
# - **Database statistics** store information about data distribution in tables and indexes.
# - The **query optimizer** uses statistics to choose the most efficient execution plan.
# - Outdated statistics can cause **poor query performance**.
# 
# ---
# 
# ### Detailed Explanation
# 
# Database statistics are **metadata** maintained by the DBMS that describe:
# - Number of rows in a table
# - Data distribution in columns
# - Index selectivity
# - Cardinality estimates
# 
# The **query optimizer** relies on these statistics to decide **how a query should be executed**.
# 
# ---
# 
# ### How Database Statistics Affect Query Performance
# 
# ---
# 
# ### 1. Query Plan Selection
# - The optimizer estimates how many rows each step of a query will return.
# - It compares multiple execution plans and chooses the **lowest-cost plan**.
# - Accurate statistics → correct estimates → faster execution.
# 
# ---
# 
# ### 2. Index Selection
# - Statistics help determine **which index is most selective**.
# - A selective index reduces row scans and improves performance.
# 
# ---
# 
# ### 3. Join Order Optimization
# - In multi-table queries, the optimizer decides **join order**.
# - Statistics on table size and data distribution guide this decision.
# - Wrong join order = unnecessary scans = slow queries.
# 
# ---
# 
# ### 4. Predicate Evaluation
# - Statistics estimate how selective conditions in the **WHERE clause** are.
# - Helps optimizer apply filters as early as possible.
# 
# ---
# 
# ### 5. Memory and Resource Allocation
# - Statistics help estimate memory and CPU needs.
# - Allows efficient allocation of resources during query execution.
# 
# ---
# 
# ### Updating Statistics
# 
# After bulk operations like **data imports**, statistics should be updated:
# 
# ```sql
# UPDATE STATISTICS table_name;
# ```
# Or for a specific index/statistics object:
# ```sql
# UPDATE STATISTICS table_name index_or_statistics_name;
# ```
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Slow query + no code change → **check statistics first**.
# 
# -   Always update statistics after **bulk inserts, updates, or deletes**.
# 
# -   Outdated statistics = wrong execution plan = poor performance.


# MARKDOWN ********************

# ## Question 25: Does the order of columns matter in a compound index?
# 
# ### Short Answer
# - **Yes**, the order of columns in a compound index matters.
# - It affects how efficiently the index is used by SQL queries.
# - Index usage follows the **left-most prefix rule**.
# 
# ---
# 
# ### Detailed Explanation
# 
# A **compound (composite) index** is an index created on multiple columns.  
# The **order of columns** determines how the index is structured and used by the query optimizer.
# 
# ---
# 
# ### Index Order Example
# 
# Consider two possible indexes:
# - **(book_id, active)**
# - **(active, book_id)**
# 
# ---
# 
# ### 1. Index on (book_id, active)
# - Best for queries that filter by **book_id**
# - Also effective when filtering by **book_id AND active**
# - Not efficient for filtering by **active alone**
# 
# **Example Query:**  
# ```sql
# SELECT *
# FROM books
# WHERE book_id = 101 AND active = 1;
# ```
# ### 2\. Index on (active, book_id)
# 
# -   Best for queries that filter by **active**
# 
# -   Also effective when filtering by **active AND book_id**
# 
# -   Not efficient for filtering by **book_id alone**
# 
# **Example Query:**
# ```sql
# SELECT *
# FROM books
# WHERE active = 1 AND book_id = 101;`
# ```
# * * * * *
# 
# ### Left-Most Prefix Rule
# 
# -   An index is used efficiently **only if the query uses the leading column(s)**.
# 
# -   `(book_id, active)` can be used for:
# 
#     -   `book_id`
# 
#     -   `book_id AND active`
# 
# -   `(active, book_id)` can be used for:
# 
#     -   `active`
# 
#     -   `active AND book_id`
# 
# * * * * *
# 
# ### Design Considerations
# 
# -   Place the **most selective or most frequently filtered column first**.
# 
# -   Avoid creating too many indexes:
# 
#     -   Increases storage
# 
#     -   Slows down INSERT, UPDATE, DELETE operations
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Always mention the **left-most prefix rule**.
# 
# -   Index column order should match **real query patterns**.
# 
# -   More indexes ≠ better performance --- balance is key.


# MARKDOWN ********************

# ## Question 26: What are `_` and `%` used for in SQL?
# 
# ### Short Answer
# - `_` and `%` are **wildcards** used with the **LIKE operator**.
# - `_` matches **exactly one character**.
# - `%` matches **any number of characters (including zero)**.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL, the `LIKE` operator is used for **pattern matching** in string
# comparisons. The `_` (underscore) and `%` (percent sign) are special
# wildcard characters that help define flexible search patterns.
# 
# ---
# 
# ### 1. Underscore (`_`) Wildcard
# - Matches **exactly one character**
# - Position-specific
# - Useful when the length of the string matters
# 
# **Example:**  
# Find names starting with `J` and having exactly 4 characters:
# ```sql
# SELECT *
# FROM employees
# WHERE name LIKE 'J___';
# ```
# This matches:
# 
# -   John
# 
# -   Jack
# 
# But not:
# 
# -   James
# 
# -   Jo
# 
# * * * * *
# 
# ### 2\. Percent (`%`) Wildcard
# 
# -   Matches **zero or more characters**
# 
# -   Can be used at the beginning, middle, or end of a pattern
# 
# **Example:**\
# Find names starting with `A`:
# ```sql
# SELECT *
# FROM employees
# WHERE name LIKE 'A%';
# ```
# **Example:**\
# Find names containing the letter `n` anywhere:
# ```sql
# SELECT *
# FROM employees
# WHERE name LIKE '%n%';`
# ```
# * * * * *
# 
# ### Case Sensitivity Note
# 
# -   `LIKE` is **case-insensitive by default in SQL Server** (based on collation).
# 
# -   For case-sensitive searches:
# 
#     -   Use a case-sensitive collation, or
# 
#     -   Apply `LOWER()` / `UPPER()` functions.
# 
# * * * * *
# 
# ### Summary Table
# 
# | Wildcard | Meaning | Example Pattern |
# | --- | --- | --- |
# | `_` | Exactly one character | `J___` |
# | `%` | Zero or more characters | `A%`, `%n%` |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Mention that `_` is **fixed-length matching**
# 
# -   `%` is **variable-length matching**
# 
# -   Both are used only with the `LIKE` operator


# MARKDOWN ********************

# ## Question 27: How do you ensure that a particular SQL query uses a specific index?
# 
# ### Short Answer
# You can **suggest or influence** index usage using **index hints**, but the
# query optimizer ultimately decides whether to use the index.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL, you **cannot fully force** a query to use a specific index because
# the **query optimizer** is responsible for selecting the most efficient
# execution plan based on cost, statistics, and data distribution.
# 
# However, you can **influence** the optimizer to prefer a particular index
# by following best practices.
# 
# ---
# 
# ### Ways to Influence Index Usage
# 
# #### 1. Create the Right Index
# Ensure indexes exist on columns used in:
# - `WHERE`
# - `JOIN`
# - `ORDER BY`
# - `GROUP BY`
# 
# A well-designed index significantly increases the likelihood of being used.
# 
# ---
# 
# #### 2. Keep Statistics Up-to-Date
# The optimizer relies on statistics to estimate row counts and costs.
# 
# ```sql
# UPDATE STATISTICS table_name;
# ```
# Outdated statistics can cause poor index selection or full table scans.
# 
# * * * * *
# 
# #### 3\. Use Index Hints (Last Resort)
# 
# Some databases allow **index hints** to suggest a specific index.
# 
# **SQL Server Example:**
# ```sql
# SELECT *
# FROM employees WITH (INDEX(idx_employee_dept))
# WHERE department_id = 10;`
# ```
# ⚠️ Use hints carefully --- they can:
# 
# -   Reduce plan flexibility
# 
# -   Break performance if data distribution changes
# 
# * * * * *
# 
# #### 4\. Rewrite the Query
# 
# Small changes in query structure can influence index usage:
# 
# -   Avoid functions on indexed columns
# 
# -   Avoid implicit data type conversions
# 
# -   Write sargable (search-argument-able) conditions
# 
# Bad:
# ```sql
# WHERE YEAR(order_date) = 2024
# ```
# Good:
# ```sql
# WHERE order_date >= '2024-01-01'
#   AND order_date < '2025-01-01'
# ```
# * * * * *
# 
# #### 5\. Use Covering Indexes
# 
# A covering index includes **all columns required by the query**, so the\
# database does not need to access the base table.
# ```sql
# CREATE INDEX idx_cover
# ON orders(customer_id)
# INCLUDE(order_date, amount);`
# ```
# * * * * *
# 
# #### 6\. Avoid Too Many Indexes
# 
# Too many indexes:
# 
# -   Increase storage
# 
# -   Slow down INSERT/UPDATE/DELETE
# 
# -   Can confuse the optimizer
# 
# Remove unused or redundant indexes.
# 
# * * * * *
# 
# ### Important Note
# 
# Even with hints, the optimizer **may ignore** the suggested index if it\
# determines another execution plan is cheaper.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Say: *"You cannot force index usage, but you can influence it."*
# 
# -   Emphasize:
# 
#     -   Proper index design
# 
#     -   Updated statistics
# 
#     -   Index hints as **last resort**


# MARKDOWN ********************

# ## Question 28: In SQL Server, which is fastest and slowest among Index Seek, Index Scan, and Table Scan?
# 
# ### Short Answer
# In general:
# **Index Seek is the fastest**, followed by **Index Scan**, and
# **Table Scan is the slowest**.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL Server, the query optimizer decides how to access data based on
# statistics, indexes, and estimated cost. The performance of a query largely
# depends on whether it uses an Index Seek, Index Scan, or Table Scan.
# 
# ---
# 
# ### 1. Index Seek (Fastest)
# - An **Index Seek** directly navigates to the required rows using an index.
# - It retrieves only the rows that match the search condition.
# - Best suited for **highly selective queries**.
# 
# Example:
# ```sql
# SELECT *
# FROM employees
# WHERE employee_id = 101;
# ```
# If `employee_id` is indexed, SQL Server can quickly locate the row using\
# an Index Seek.
# 
# * * * * *
# 
# ### 2\. Index Scan (Medium)
# 
# -   An **Index Scan** reads all rows (or a range of rows) from an index.
# 
# -   Faster than a Table Scan because indexes are usually smaller than tables.
# 
# -   Occurs when:
# 
#     -   The query returns many rows
# 
#     -   No selective filter exists
# 
# Example:
# ```sql
# SELECT *
# FROM employees
# WHERE department_id > 0;
# ```
# This may scan a large portion of the index.
# 
# * * * * *
# 
# ### 3\. Table Scan (Slowest)
# 
# -   A **Table Scan** reads every row in the table.
# 
# -   SQL Server checks each row to see if it matches the condition.
# 
# -   Very expensive for large tables.
# 
# Occurs when:
# 
# -   No suitable index exists
# 
# -   Statistics are outdated
# 
# -   Query is not sargable
# 
# Example:
# ```sql
# SELECT *
# FROM employees
# WHERE UPPER(name) = 'JOHN';
# ```
# Using a function on the column can prevent index usage, leading to a table scan.
# 
# * * * * *
# 
# ### Performance Order (Fast → Slow)
# 
# | Operation | Performance |
# | --- | --- |
# | Index Seek | Fastest |
# | Index Scan | Medium |
# | Table Scan | Slowest |
# 
# * * * * *
# 
# ### How to Check Which One Is Used
# 
# You can inspect the execution plan:
# ```sql
# SET STATISTICS PROFILE ON;
# -- or
# SET STATISTICS IO ON;
# ```
# Or simply view the **Actual Execution Plan** in SQL Server Management Studio.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Say: **"Index Seek is ideal, Index Scan is acceptable, Table Scan should be avoided for large tables."**
# 
# -   Emphasize:
# 
#     -   Proper indexing
# 
#     -   Updated statistics
# 
#     -   Writing sargable queries
# 
# * * * * *
# 
# ### Key Takeaway
# 
# Good indexing + good query design = **Index Seeks**\
# Poor indexing or bad query design = **Table Scans**


# MARKDOWN ********************

# ## Question 29: What does NULL = NULL return in SQL?
# 
# ### Short Answer
# - `NULL = NULL` returns **NULL**, not TRUE.
# - SQL uses **three-valued logic**: TRUE, FALSE, NULL (UNKNOWN).
# - Comparisons involving NULL cannot be evaluated with standard operators.
# 
# ---
# 
# ### Detailed Explanation
# 
# - **NULL** represents an unknown or missing value.
# - SQL cannot determine equality or inequality between two unknown values.
# - Hence, `NULL = NULL` evaluates to **NULL** (UNKNOWN) rather than TRUE.
# - Similarly, `NULL <> NULL` also evaluates to NULL.
# 
# ---
# 
# ### Correct Way to Check for NULL Values
# - **IS NULL**: Checks if a value is NULL.
# - **IS NOT NULL**: Checks if a value is not NULL.
# 
# **Example:**
# ```sql
# SELECT *
# FROM employees
# WHERE department_id IS NULL;
# ```
# This query correctly retrieves all rows where `department_id` is NULL.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Never use `=` or `<>` to check for NULL values.
# 
# -   Always use **`IS NULL`** or **`IS NOT NULL`** for proper NULL handling.
# 
# -   Understand three-valued logic to avoid logical errors in queries.


# MARKDOWN ********************

# ## Question 30: Write SQL query to find all rows where EMP_NAME is NULL
# 
# ### Short Answer
# - Use the **`IS NULL`** operator to check for NULL values.
# - `=` operator **does not work** for NULL comparisons.
# 
# ---
# 
# ### Detailed Explanation
# - **NULL** represents unknown or missing data.
# - Standard comparison operators like `=` or `<>` cannot detect NULL values.
# - **`IS NULL`** is the correct method to find rows with NULL in a column.
# - **`IS NOT NULL`** is used to find rows with actual values (non-NULL).
# 
# ---
# 
# ### Example SQL Query
# ```sql
# SELECT *
# FROM employees
# WHERE EMP_NAME IS NULL;
# ```
# This query retrieves all rows where the `EMP_NAME` column has no value.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Always use **`IS NULL`** or **`IS NOT NULL`** for NULL checks.
# 
# -   Never attempt `EMP_NAME = NULL` or `EMP_NAME <> NULL`.
# 
# -   Understanding NULL handling is crucial for avoiding logical errors in queries.

# MARKDOWN ********************

# ## Question 31: What is the temp table?
# 
# ### Short Answer
# - A **temp table** (temporary table) exists only during the **current database session**.
# - Once the session ends, the table and its data are automatically dropped.
# - Unlike a view, a temp table can be used like a **regular table** throughout the session.
# 
# ---
# 
# ### Detailed Explanation
# - Temporary tables are used to store **intermediate results** or **temporary data** during query execution or stored procedures.
# - They help break complex operations into **manageable steps** and improve performance.
# 
# ---
# 
# ### Types of Temporary Tables
# 
# 1. **Local Temporary Table (`#temp_table`)**
# - Visible only within the session that created it.
# - Automatically dropped when the session ends.
# 
# 2. **Global Temporary Table (`##temp_table`)**
# - Visible across sessions in the same database.
# - Dropped automatically when the last session using it ends.
# 
# ---
# 
# ### Example SQL Queries
# 
# **Create a Local Temp Table**
# ```sql
# CREATE TABLE #TempEmployees (
#     Emp_ID INT,
#     Emp_Name VARCHAR(50),
#     Dept VARCHAR(50)
# );
# 
# INSERT INTO #TempEmployees VALUES (1, 'John', 'IT');
# SELECT * FROM #TempEmployees;
# ```
# **Create a Global Temp Table**
# ```sql
# CREATE TABLE ##TempDepartments (
#     Dept_ID INT,
#     Dept_Name VARCHAR(50)
# );
# 
# INSERT INTO ##TempDepartments VALUES (101, 'IT');
# SELECT * FROM ##TempDepartments;
# ```
# **Drop Temp Table (optional)**
# ```sql
# DROP TABLE #TempEmployees;
# DROP TABLE ##TempDepartments;
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   **Local temp tables (`#`)** → session-specific, automatically dropped.
# 
# -   **Global temp tables (`##`)** → shared across sessions, dropped after last reference.
# 
# -   Use temp tables for **intermediate results**, not for long-term storage.


# MARKDOWN ********************

# ## Question 32: What is the fastest way to empty or clear a table?
# 
# ### Short Answer
# - Use the **TRUNCATE TABLE** command to quickly empty a table.
# - Faster than DELETE because it doesn't log each deleted row.
# - Cannot be rolled back, so use with caution.
# 
# ---
# 
# ### Detailed Explanation
# - **TRUNCATE TABLE** removes all rows efficiently and quickly.
# - It deallocates the data pages directly and logs minimal information.
# - Typically faster than DELETE, especially for large tables.
# 
# **Syntax:**
# ```sql
# TRUNCATE TABLE table_name;
# ```
# Replace `table_name` with your table's name.
# 
# * * * * *
# 
# ### Advantages of TRUNCATE TABLE
# 
# 1.  **Speed**: Directly deallocates pages instead of deleting row by row.
# 
# 2.  **Minimal Logging**: Logs only page deallocations, saving log space.
# 
# 3.  **No Rollback**: Cannot be rolled back; operation is immediate.
# 
# * * * * *
# 
# ### Considerations
# 
# -   **Cannot use with foreign keys**: Tables referenced by foreign keys cannot be truncated.
# 
# -   **Resets identity columns**: Identity seeds are reset to their original starting value.
# 
# -   **Requires permissions**: Only users with the right privileges can truncate.
# 
# * * * * *
# 
# ### Example
# ```sql
# -- Quickly empty the Employees table
# TRUNCATE TABLE Employees;
# ```
# **Comparison with DELETE**
# 
# -   `DELETE FROM Employees;` → Slower, logs each row, can rollback.
# 
# -   `TRUNCATE TABLE Employees;` → Fast, minimal logging, cannot rollback.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Use **TRUNCATE** for quick clearing of large tables.
# 
# -   Use **DELETE** if you need rollback or have foreign key dependencies.


# MARKDOWN ********************

# ## Question 33: What is an identity column in SQL Server? How do you return an identity value?
# 
# ### Short Answer
# - An **identity column** automatically generates unique numeric values for each new row.
# - Often used as a **primary key**.
# - Defined using `IDENTITY(seed, increment)`.
# 
# ---
# 
# ### Detailed Explanation
# - **Identity Column** automatically assigns unique, sequential values.
# - Syntax for creating an identity column:
# ```sql
# CREATE TABLE Employees (
#     Emp_ID INT IDENTITY(1,1) PRIMARY KEY,
#     Emp_Name VARCHAR(50),
#     Dept VARCHAR(50)
# );
# ```
# -   Here, `Emp_ID` starts at 1 and increments by 1 for each new row.
# 
# * * * * *
# 
# ### Returning Identity Values
# 
# 1.  **@@IDENTITY**
# ```sql
# INSERT INTO Employees (Emp_Name, Dept)
# VALUES ('John', 'IT');
# 
# SELECT @@IDENTITY AS LastIdentity;
# ```
# -   Returns the last identity value generated in the session.
# 
# -   Be cautious: triggers may affect the value.
# 
# 2.  **SCOPE_IDENTITY()**
# ```sql
# INSERT INTO Employees (Emp_Name, Dept)
# VALUES ('Mary', 'HR');
# 
# SELECT SCOPE_IDENTITY() AS LastIdentity;
# ```
# -   Returns the last identity value generated **within the current scope**.
# 
# -   Safer than `@@IDENTITY`.
# 
# 3.  **OUTPUT Clause**
# ```sql
# INSERT INTO Employees (Emp_Name, Dept)
# OUTPUT INSERTED.Emp_ID
# VALUES ('Alice', 'Finance');
# ```
# -   Returns the identity value(s) directly from the `INSERT` statement.
# 
# -   Useful when inserting multiple rows.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Use `SCOPE_IDENTITY()` for safe retrieval of identity values after an insert.
# 
# -   Use the `OUTPUT` clause if you need immediate feedback or are inserting multiple rows.


# MARKDOWN ********************

# ## Question 34: How do you return an identity value from a table with a trigger?
# 
# ### Short Answer
# - In SQL Server, you can use functions like `@@IDENTITY` or `SCOPE_IDENTITY()` to return identity values.
# - **Caution:** `@@IDENTITY` may return identity values generated by triggers, not just your insert.
# 
# ---
# 
# ### Detailed Explanation
# - **Triggers** can insert rows into other tables with identity columns.
# - `@@IDENTITY` returns the last identity in the **current session**, including triggers.
# - `SCOPE_IDENTITY()` returns the last identity in the **current scope**, ignoring triggers.
# - `OUTPUT` clause can also safely return inserted identity values.
# 
# ---
# ### Interview Tip
# 
# -   Prefer `SCOPE_IDENTITY()` over `@@IDENTITY` when triggers are present.
# 
# -   The `OUTPUT` clause is ideal for retrieving identity values safely, especially when inserting multiple rows.

# MARKDOWN ********************

# ## Question 35: How do you return a value from a stored procedure?
# 
# ### Short Answer
# - In SQL Server, you can return values from a stored procedure using:
#   - The **RETURN** statement
#   - **OUTPUT parameters**
# 
# ---
# 
# ### Detailed Explanation
# 
# SQL Server provides two ways to return values from a stored procedure, and each serves a different purpose.
# 
# ---
# 
# ### 1. Using the RETURN Statement
# - Used to return **only one integer value**.
# - Commonly used to indicate **execution status**.
# - By convention:
#   - `0` → Success
#   - Non-zero → Error or warning
# - Cannot return strings or multiple values.
# 
# **Example:**  
# ```sql
# CREATE PROCEDURE GetEmployeeCount
# AS
# BEGIN
#     DECLARE @EmployeeCount INT;
#     SELECT @EmployeeCount = COUNT(*) FROM Employees;
#     RETURN @EmployeeCount;
# END;
# ```
# **Calling the Procedure:**
# ```sql
# DECLARE @Result INT;
# EXEC @Result = GetEmployeeCount;
# PRINT @Result;
# ```
# * * * * *
# 
# ### 2\. Using OUTPUT Parameters
# 
# -   Used to return **multiple values**.
# 
# -   Can return **any data type**.
# 
# -   Commonly used for returning query results or calculated values.
# 
# **Example:**
# ```sql
# CREATE PROCEDURE GetEmployeeDetails
#     @EmployeeID INT,
#     @FirstName NVARCHAR(50) OUTPUT,
#     @Title NVARCHAR(50) OUTPUT
# AS
# BEGIN
#     SELECT
#         @FirstName = FirstName,
#         @Title = Title
#     FROM Employees
#     WHERE EmployeeID = @EmployeeID;
# END;
# ```
# **Calling the Procedure:**
# ```sql
# DECLARE @Name NVARCHAR(50), @Position NVARCHAR(50);
# 
# EXEC GetEmployeeDetails
#     @EmployeeID = 1,
#     @FirstName = @Name OUTPUT,
#     @Title = @Position OUTPUT;
# 
# PRINT @Name;
# PRINT @Position;
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   **RETURN** → single integer → status code
# 
# -   **OUTPUT parameters** → multiple values → real data
# 
# -   Prefer **OUTPUT parameters** for practical, production-level stored procedures.


# MARKDOWN ********************

# ## Question 36: How do you return a VARCHAR value from a stored procedure?
# 
# ### Short Answer
# - A stored procedure cannot return a VARCHAR value using the **RETURN** statement.
# - To return a VARCHAR value, you must use an **OUTPUT parameter**.
# 
# ---
# 
# ### Detailed Explanation
# 
# In SQL Server, the **RETURN** statement can only return an **integer value**.  
# To return a **VARCHAR or any non-integer data type**, an **OUTPUT parameter** must be used.
# 
# OUTPUT parameters allow stored procedures to pass values back to the calling program and support all data types, including VARCHAR.
# 
# ---
# 
# ### Using OUTPUT Parameter to Return VARCHAR
# 
# **Stored Procedure:**  
# ```sql
# CREATE PROCEDURE FetchLastName
#     @EmployeeID INT,
#     @LastName VARCHAR(50) OUTPUT
# AS
# BEGIN
#     SELECT @LastName = LastName
#     FROM [Northwind].dbo.Employees
#     WHERE EmployeeID = @EmployeeID;
# END;
# ```
# **Calling the Stored Procedure:**
# ```sql
# DECLARE @Name VARCHAR(50);
# 
# EXEC FetchLastName
#     @EmployeeID = 4,
#     @LastName = @Name OUTPUT;
# 
# PRINT @Name;
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   **RETURN** → Only integer values (status codes)
# 
# -   **OUTPUT parameters** → Strings, dates, multiple values
# 
# -   To return a **VARCHAR**, always use an **OUTPUT parameter**


# MARKDOWN ********************

# ## Question 37: If you have a column that will only have values between 1 and 250, what data type will you use?
# 
# ### Answer
# If you are using **SQL Server**, the best data type to use is **TINYINT**.
# 
# ---
# 
# ### Explanation
# 
# The **TINYINT** data type in SQL Server:
# - Stores values from **0 to 255**
# - Uses **1 byte of storage**
# - Is ideal for small numeric ranges
# 
# Since the column values are guaranteed to be between **1 and 250**, **TINYINT** is the most efficient and appropriate choice.
# 
# ---
# 
# ### Why This Question Is Asked in Interviews
# 
# This question checks:
# - Knowledge of SQL Server data types
# - Ability to choose the most efficient data type
# - Awareness of storage optimization
# 
# Your goal should always be to:
# - Choose the **smallest data type** that satisfies the requirement
# - Consider whether the range might change in the future
# 
# ---
# 
# ### Comparison
# 
# | Data Type | Range | Storage |
# |----------|-------|---------|
# | TINYINT  | 0 to 255 | 1 byte |
# | SMALLINT | -32,768 to 32,767 | 2 bytes |
# | INT      | -2B to +2B | 4 bytes |
# 
# ---
# 
# ### Final Verdict
# ✔ **TINYINT** is the correct and optimal choice for values between **1 and 250**


# MARKDOWN ********************

# ## Question 38: Difference between LEFT and RIGHT OUTER JOIN in SQL?
# 
# ### Answer
# Both **LEFT OUTER JOIN** and **RIGHT OUTER JOIN** are outer joins that return
# matching and non-matching rows, but they differ in which table’s rows are
# fully included.
# 
# ---
# 
# ### LEFT OUTER JOIN
# - Returns **all rows from the LEFT table**
# - Returns **only matching rows from the RIGHT table**
# - If no match exists, columns from the right table contain **NULL**
# 
# #### Example
# ```sql
# SELECT *
# FROM Employees E
# LEFT JOIN Departments D
# ON E.DeptID = D.DeptID;
# ```
# ### RIGHT OUTER JOIN
# 
# -   Returns **all rows from the RIGHT table**
# 
# -   Returns **only matching rows from the LEFT table**
# 
# -   If no match exists, columns from the left table contain **NULL**
# 
# #### Example
# ```sql
# SELECT *
# FROM Employees E
# RIGHT JOIN Departments D
# ON E.DeptID = D.DeptID;
# ```
# * * * * *
# 
# ### Key Difference Summary
# 
# | Feature | LEFT OUTER JOIN | RIGHT OUTER JOIN |
# | --- | --- | --- |
# | All rows returned from | Left table | Right table |
# | Matching rows from | Right table | Left table |
# | Non-matching side shows | NULLs on right | NULLs on left |
# 
# * * * * *
# 
# ### Interview Tip
# 
# ✔ **LEFT JOIN is preferred** in most real-world queries\
# ✔ Any `RIGHT JOIN` can be rewritten as a `LEFT JOIN` by swapping table positions
# 
# * * * * *
# 
# ### Final Verdict
# 
# -   Use **LEFT OUTER JOIN** when you want all records from the left table
# 
# -   Use **RIGHT OUTER JOIN** when you want all records from the right table


# MARKDOWN ********************

# ## Question 39: Can you write an SQL query to select all last names that start with 'T'?
# 
# ### Answer
# You can use the **LIKE** operator with a wildcard to filter values that start
# with a specific character.
# 
# ### SQL Query
# ```sql
# SELECT LastName
# FROM Employees
# WHERE LastName LIKE 'T%';
# ```
# ### Explanation
# 
# -   The `SELECT` statement retrieves the **LastName** column from the\
#     **Employees** table.
# 
# -   The `WHERE` clause filters rows where **LastName** starts with the\
#     letter **'T'**.
# 
# -   The `%` wildcard represents **zero or more characters** after `T`.
# 
# ### Result
# 
# This query returns **all last names that begin with the letter 'T'**.

# MARKDOWN ********************

# # Question 40  
# ## How would you select all rows where the date is 20231002?
# 
# ### Short Answer
# You can select rows using a `WHERE` clause by comparing the date column
# with the required date value.
# 
# ---
# 
# ### SQL Query (Recommended – DATE column)
# 
# ```sql
# SELECT *
# FROM your_table
# WHERE date_column = '2023-10-02';
# ```
# ### Explanation
# 
# -   Replace **`your_table`** with the actual table name.
# 
# -   Replace **`date_column`** with the column that stores date values.
# 
# -   `'2023-10-02'` is the standard **ISO date format (YYYY-MM-DD)** and is\
#     recommended for SQL Server and most databases.
# 
# * * * * *
# 
# ### Important Interview Tip ⚠️
# 
# If the column is of type **DATETIME** (contains time part), the above query\
# may not return results because the time value will not match exactly.
# 
# In that case, use a date range:
# ```sql
# SELECT *
# FROM your_table
# WHERE date_column >= '2023-10-02'
#   AND date_column < '2023-10-03';
# ```
# * * * * *
# 
# ### Key Takeaways
# 
# -   Always use **ISO date format (`YYYY-MM-DD`)**
# 
# -   Be careful with **DATE vs DATETIME**
# 
# -   Use a **date range** when time is present to avoid missing records


# MARKDOWN ********************

# # Question 41  
# ## What is the difference between a local and global temporary table in SQL Server?
# 
# ### Short Answer
# - **Local Temporary Table**: Prefixed with a single `#`, visible only within the session that created it.  
# - **Global Temporary Table**: Prefixed with `##`, visible across all sessions in the database.
# 
# ---
# 
# ### Long Answer
# 
# In SQL Server, **temporary tables** are used to store intermediate or temporary data.  
# They come in **two types**: local and global, with differences in scope and lifespan.
# 
# ---
# 
# ### 1. Local Temporary Table
# - Created with a **single hash (#)** prefix: `#temp_table`
# - Scope: **Visible only in the session (connection) that created it**
# - Lifespan: **Automatically dropped** when the session ends or explicitly dropped
# - Use Case: Temporary data storage within **stored procedures**, **functions**, or a single user session
# 
# ```sql
# -- Example of Local Temp Table
# CREATE TABLE #LocalTemp(
#     ID INT,
#     Name NVARCHAR(50)
# );
# 
# INSERT INTO #LocalTemp VALUES (1, 'John');
# SELECT * FROM #LocalTemp;
# 
# -- Table will be automatically dropped when session ends
# ```
# ### 2\. Global Temporary Table
# 
# -   Created with **double hash (##)** prefix: `##GlobalTemp`
# 
# -   Scope: **Visible across multiple sessions** in the same database
# 
# -   Lifespan: **Dropped when the last session referencing it closes** or explicitly dropped
# 
# -   Use Case: Sharing temporary data between **different sessions or connections**
# ```sql
# -- Example of Global Temp Table
# CREATE TABLE ##GlobalTemp(
#     ID INT,
#     Name NVARCHAR(50)
# );
# 
# INSERT INTO ##GlobalTemp VALUES (1, 'Mary');
# SELECT * FROM ##GlobalTemp;
# 
# -- Table persists until the last referencing session ends
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Feature | Local Temp Table (#) | Global Temp Table (##) |
# | --- | --- | --- |
# | Scope | Single session | Multiple sessions |
# | Lifespan | Ends with session | Ends when last session ends |
# | Visibility | Only in session that created it | Accessible to all sessions in DB |
# | Use Case | Session-specific temporary data | Shared temporary data between sessions |
# 
# * * * * *
# 
# ### Interview Tip ⚡
# 
# -   Use **local temp tables** for session-specific computations.
# 
# -   Use **global temp tables** for sharing temporary results across sessions, but be cautious of conflicts with other users.


# MARKDOWN ********************

# # Question 42  
# ## How do you create a copy of a table in SQL Server?
# 
# ### Short Answer
# You can create a copy of a table using either:
# 1. `SELECT INTO` — creates a new table with the same structure and copies data  
# 2. `INSERT INTO` — copies data into an existing table  
# 
# ---
# 
# ### Long Answer
# 
# In SQL Server, creating a copy of a table depends on whether you want to **create a new table** or **copy data into an existing table**.  
# 
# ---
# 
# ### 1. Using `SELECT INTO` Statement
# - Creates a **new table** with the same structure as the source table
# - Copies all the data from the source table into the new table
# 
# ```sql
# -- Example: Create a copy of old_table into new_table
# SELECT *
# INTO new_table
# FROM old_table;
# ```
# **Notes:**
# 
# -   `new_table` will automatically inherit the column names and data types from `old_table`
# 
# -   Constraints (primary key, foreign key, indexes) are **not copied** with this method
# 
# -   Very useful for quickly creating a backup or temporary copy of a table
# 
# * * * * *
# 
# ### 2\. Using `INSERT INTO` Statement
# 
# -   Copies data from the source table into an **already existing table**
# 
# -   The existing table must have the same column structure (or compatible columns)
# ```sql
# -- Step 1: Create a new table (structure only)
# CREATE TABLE new_table (
#     ID INT,
#     Name NVARCHAR(50),
#     Salary DECIMAL(10,2)
# );
# 
# -- Step 2: Insert data from old_table
# INSERT INTO new_table
# SELECT *
# FROM old_table;
# ```
# **Notes:**
# 
# -   Useful when you already have a table created with specific constraints, indexes, or data types
# 
# -   Preserves existing table definitions like primary keys, defaults, and indexes
# 
# * * * * *
# 
# ### Summary
# 
# | Method | Creates New Table? | Copies Data? | Copies Constraints/Indexes? |
# | --- | --- | --- | --- |
# | `SELECT INTO` | Yes | Yes | No |
# | `INSERT INTO` | No (existing table required) | Yes | Yes, if table already has them |
# 
# **Pro Tip:**
# 
# -   Use `SELECT INTO` for a **quick copy without constraints**
# 
# -   Use `INSERT INTO` if you need **full control over table structure and constraints**


# MARKDOWN ********************

# Question 43: How to Change the Data Type of a Column in SQL
# -----------------------------------------------------------
# 
# ### Short Answer
# 
# -   Use **ALTER TABLE** with **ALTER COLUMN** to modify a column's data type.
# 
# -   Syntax: `ALTER TABLE table_name ALTER COLUMN column_name new_data_type;`
# 
# -   Changing data types may cause **data loss**, affect **constraints/indexes**, or require **downtime**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Altering a column's data type lets you adjust the kind of data a column can store.\
# Different database systems may have **slightly different syntax** and limitations, so always check your DBMS documentation.
# 
# * * * * *
# 
# ### 1\. ALTER TABLE with ALTER COLUMN
# 
# -   **ALTER TABLE** specifies the table to modify.
# 
# -   **ALTER COLUMN** identifies the column and the new data type.
# 
# -   Works for changing numeric, string, date, or other types depending on DBMS.
# 
# **Example:**
# ```sql
# ALTER TABLE Employees
# ALTER COLUMN Age INT;
# ```
# ### 2\. Considerations When Changing Data Types
# 
# -   **Data Conversion:** Existing values might be **truncated or lost** if incompatible with the new type.
# 
# -   **Constraints and Indexes:** Columns with **PRIMARY KEY, FOREIGN KEY, UNIQUE, or INDEX** may need to be **dropped and recreated**.
# 
# -   **Potential Downtime:** Large tables may take **significant time** to process, causing temporary unavailability.
# 
# -   **Testing:** Always perform changes in a **test environment** and **back up data** first.
# 
# -   **Impacted Queries:** Queries, stored procedures, and applications using the column may require **updates** to accommodate the new type.
# 
# ### Interview Tip
# 
# -   Always **backup** before altering data types.
# 
# -   Be aware of **data loss, constraints, and application impacts**.
# 
# -   For large tables, consider **off-peak maintenance windows**.


# MARKDOWN ********************

# Question 44: What Data Type Should You Use to Store Monetary Values in SQL
# --------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   Use **DECIMAL** (or **NUMERIC**) for monetary values.
# 
# -   **DECIMAL(p, s)** specifies **precision** (total digits) and **scale** (digits after decimal).
# 
# -   Avoid **FLOAT** or **REAL** to prevent **rounding errors**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Monetary values require **precise decimal representation**.\
# **DECIMAL/NUMERIC** are fixed-point data types designed for **accurate calculations**, unlike floating-point types which can introduce rounding errors.
# 
# * * * * *
# 
# ### 1\. DECIMAL Data Type
# 
# -   **Precision (p):** Total number of digits stored (left + right of decimal).
# 
# -   **Scale (s):** Number of digits **after the decimal point**.
# 
# -   Suitable for **financial and accounting applications**.
# 
# **Example:**
# ```sql
# CREATE TABLE FinancialData (
#    TransactionID INT PRIMARY KEY,
#    Amount DECIMAL(10,2)
# );
# ```
# -   Here, `Amount` can store values up to **99999999.99**.
# 
# ### 2\. Why Not FLOAT or REAL?
# 
# -   **FLOAT/REAL** are **approximate** data types.
# 
# -   Can produce **rounding errors** in calculations.
# 
# -   Not reliable for **money, accounting, or precise calculations**.
# 
# ### Interview Tip
# 
# -   Always use **DECIMAL or NUMERIC** for money.
# 
# -   Choose **precision and scale** according to the **maximum expected value**.
# 
# -   Avoid FLOAT/REAL for financial calculations to ensure **accuracy**.


# MARKDOWN ********************

# Question 45: What Does `SELECT 3/2` Return in SQL -- 1 or 1.5?
# -------------------------------------------------------------
# 
# ### Short Answer
# 
# -   **`SELECT 3/2`** returns **1** in SQL when both operands are integers.
# 
# -   Fractional parts are **truncated** in integer division.
# 
# -   To get a decimal result, use **decimal or floating-point literals**: `3.0/2` or `3/2.0` → **1.5**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# In SQL, arithmetic operations depend on the **data types of the operands**:
# 
# -   **Integer ÷ Integer** → integer division (fractional part truncated).
# 
# -   **Decimal/Float ÷ Any Number** → decimal division (fraction preserved).
# 
# * * * * *
# 
# ### 1\. Integer Division
# 
# -   Occurs when **both numbers are integers**.
# 
# -   Truncates the **decimal part**, returning only the integer portion.
# 
# **Example:**
# ```sql
# SELECT 3 / 2 AS Result;
# -- Returns 1
# ```
# ### 2\. Decimal or Floating-Point Division
# 
# -   Use **decimal literals** (`3.0`) or **cast integers** to decimal.
# 
# -   Preserves fractional results.
# 
# **Examples:**
# ```sql
# SELECT 3.0 / 2 AS Result;   -- Returns 1.5
# SELECT 3 / 2.0 AS Result;   -- Returns 1.5
# SELECT CAST(3 AS DECIMAL) / 2 AS Result; -- Returns 1.5
# ```
# ### Interview Tip
# 
# -   Always **check operand types** in SQL division.
# 
# -   Integer ÷ Integer → **truncated result**.
# 
# -   Use **decimal literals or CAST** to get accurate fractional results.


# MARKDOWN ********************

# Question 46: Maximum Value of DECIMAL(6,5) in SQL Server
# --------------------------------------------------------
# 
# ### Short Answer
# 
# -   **DECIMAL(p, s)** stores numbers with **fixed precision and scale**.
# 
# -   **DECIMAL(6,5)** → **6 total digits**, **5 decimal places**.
# 
# -   Maximum value = **0.99999**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# **DECIMAL(p, s)** defines:
# 
# -   **Precision (p):** Total digits allowed (left + right of decimal).
# 
# -   **Scale (s):** Digits allowed **after the decimal point**.
# 
# For **DECIMAL(6,5)**:
# 
# -   Precision = 6
# 
# -   Scale = 5
# 
# -   Integer digits = 6 - 5 = 1
# 
# -   Maximum value occurs when all digits are **9** in decimal places: **0.99999**
# 
# * * * * *
# 
# ### 1\. Understanding Precision and Scale
# 
# -   **Precision (p):** Total number of digits (max size of the number).
# 
# -   **Scale (s):** Number of digits **to the right of the decimal**.
# 
# -   Formula: `Integer digits = Precision - Scale`
# 
# **Example:**
# ```sql
# DECLARE @Value DECIMAL(6,5);
# SET @Value = 0.99999;
# SELECT @Value AS MaxValue;
# -- Returns 0.99999
# ```
# ### 2\. Notes
# 
# -   Changing precision or scale changes **max/min values**.
# 
# -   Storage depends on precision (SQL Server uses 5--17 bytes depending on precision).
# 
# -   Negative numbers allowed: min value = **-0.99999**
# 
# ### Interview Tip
# 
# -   Always compute **integer digits = precision - scale**.
# 
# -   For monetary or fractional values, carefully choose **precision and scale**.
# 
# -   Remember: **DECIMAL stores exact values**, unlike FLOAT or REAL.


# MARKDOWN ********************

# Question 47: What Does `SELECT * FROM TestTable WHERE ID != 101` Return When Table Has 101, 201, and NULL?
# ----------------------------------------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   Returns only rows where **ID is not 101**.
# 
# -   **NULL comparisons** result in **Unknown**, so rows with NULL are **excluded**.
# 
# -   Result for the table `[101, 201, NULL]` → **one row with ID = 201**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# In SQL, comparisons follow **three-valued logic**: **True, False, Unknown**.
# 
# -   `ID != 101` is evaluated as:
# 
#     -   101 != 101 → **False**
# 
#     -   201 != 101 → **True**
# 
#     -   NULL != 101 → **Unknown**
# 
# -   SQL **only includes rows where the condition evaluates to True**.
# 
# -   Rows evaluating to False or Unknown are **excluded**.
# 
# * * * * *
# 
# ### 1\. Handling NULL in Comparisons
# 
# -   Any comparison with **NULL** (`=`, `!=`, `<`, `>`, etc.) returns **Unknown**.
# 
# -   Unknown rows **do not appear** in the result set.
# 
# **Example Table:**
# 
# | ID |
# | --- |
# | 101 |
# | 201 |
# | NULL |
# 
# **Query:**
# ```sql
# SELECT *
# FROM TestTable
# WHERE ID != 101;
# ```
# **Result:**
# 
# | ID |
# | --- |
# | 201 |
# 
# ### 2\. Including NULLs Explicitly
# 
# -   To include NULLs, use **`IS NULL`** or **`OR ID IS NULL`**:
# ```sql
# SELECT *
# FROM TestTable
# WHERE ID != 101 OR ID IS NULL;
# ```
# **Result:**
# 
# | ID |
# | --- |
# | 201 |
# | NULL |
# 
# ### Interview Tip
# 
# -   Always remember **NULL is not equal to anything**, even itself.
# 
# -   Comparisons with NULL → **Unknown** → excluded from query results unless explicitly handled.
# 
# -   Use **`IS NULL`** or **`IS NOT NULL`** to include/exclude NULLs in conditions.


# MARKDOWN ********************

# Question 48: What Is Your Favorite SQL Book?
# --------------------------------------------
# 
# ### Short Answer
# 
# -   Interviewers ask this to **check your familiarity with SQL resources**.
# 
# -   Name a book you have **read or are reading**.
# 
# -   If you haven't read any, consider:
# 
#     -   **Head First SQL** -- great for beginners learning SQL from scratch.
# 
#     -   **Joe Celko's SQL Puzzles and Answers** -- ideal for practicing complex queries.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# This is a **soft interview question**, not a technical one.
# 
# -   Shows your **learning attitude** and **interest in improving SQL skills**.
# 
# -   Having a book reference demonstrates that you **actively study SQL** and are aware of reliable resources.
# 
# * * * * *
# 
# ### 1\. Recommended Books
# 
# -   **Head First SQL**
# 
#     -   Beginner-friendly.
# 
#     -   Covers basic concepts like SELECT, JOINs, aggregation, and subqueries.
# 
# -   **Joe Celko's SQL Puzzles and Answers**
# 
#     -   Advanced.
# 
#     -   Focuses on **problem-solving** and **complex queries**.
# 
#     -   Helps improve logical thinking and query optimization skills.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Mention a book you **actually know** and can discuss briefly.
# 
# -   Shows that you **actively practice SQL** and are curious about improving your skills.
# 
# -   Even if you haven't read any, **naming a recommended book** shows initiative.


# MARKDOWN ********************

# Question 49: Tell Me Two SQL Best Practices You Follow
# ------------------------------------------------------
# 
# ### Short Answer
# 
# -   **Creating and using indexes** to improve query performance.
# 
# -   **Normalization** to organize data efficiently.
# 
# -   **Updating statistics** regularly for query optimization.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Following SQL best practices ensures **better performance, security, and maintainability**. Two widely recommended practices are:
# 
# * * * * *
# 
# ### 1\. Parameterized Queries
# 
# -   Always use **parameterized queries** or **prepared statements** to prevent **SQL injection attacks**.
# 
# -   Pass user inputs as **parameters**, not directly in SQL statements.
# 
# -   Benefits:
# 
#     -   Enhances **security** by blocking malicious inputs.
# 
#     -   Improves **query performance** through cached execution plans.
# 
# **Example (SQL Server, T-SQL):**
# ```sql
# DECLARE @name VARCHAR(50);
# SET @name = 'Fuller';
# SELECT * FROM Employees
# WHERE LastName = @name;
# ```
# * * * * *
# 
# ### 2\. Use Indexes Wisely
# 
# -   Apply **indexes** on columns frequently used in **search, join, or filter operations**.
# 
# -   Benefits:
# 
#     -   Reduces **full table scans**, improving query performance.
# 
#     -   Helps optimize **JOINs and WHERE filters**.
# 
# -   Caution:
# 
#     -   Excessive indexing increases **storage requirements** and **slows write operations**.
# 
#     -   Monitor **query execution plans** to ensure effective index usage.
# 
# **Example:**
# ```sql
# CREATE INDEX idx_LastName ON Employees(LastName);
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   Always mention **both security and performance practices**.
# 
# -   Parameterized queries → **prevent SQL injection**.
# 
# -   Indexes → **speed up queries**, but balance to avoid overhead.
# 
# -   Normalization and updating statistics → ensure **maintainability and efficiency**.


# MARKDOWN ********************

# Question 50: What Are the Different Isolation Levels in Microsoft SQL Server?
# -----------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   **Isolation levels** define how transactions **interact in a multi-user environment**.
# 
# -   Standard levels in SQL Server:
# 
#     1.  **READ UNCOMMITTED** -- least restrictive, allows **dirty reads**.
# 
#     2.  **READ COMMITTED** -- default, reads only **committed data**.
# 
#     3.  **REPEATABLE READ** -- prevents updates to rows read by the transaction.
# 
#     4.  **SERIALIZABLE** -- highest isolation, prevents inserts, updates, deletes affecting the result set.
# 
#     5.  **SNAPSHOT** -- uses **row versioning**, sees a consistent snapshot.
# 
#     6.  **READ COMMITTED SNAPSHOT** -- **row versioning** with committed-read behavior.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Isolation levels control **how concurrent transactions affect each other** and the **consistency of data**.\
# They balance **data integrity** and **concurrency**. SQL Server provides **six main isolation levels**, each with its own behavior:
# 
# * * * * *
# 
# ### 1\. READ UNCOMMITTED
# 
# -   Least restrictive isolation.
# 
# -   Transactions can read **uncommitted changes** from others (**dirty reads**).
# 
# -   May cause **non-repeatable reads** and **phantom reads**.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### 2\. READ COMMITTED
# 
# -   Default SQL Server isolation level.
# 
# -   Reads only **committed data**, avoiding **dirty reads**.
# 
# -   May still experience **non-repeatable reads** and **phantom reads**.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### 3\. REPEATABLE READ
# 
# -   Prevents **other transactions from updating or deleting rows** read by the current transaction.
# 
# -   Guarantees **consistent reads** but **phantom inserts** are still possible.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### 4\. SERIALIZABLE
# 
# -   **Highest isolation level**.
# 
# -   Prevents **updates, deletes, or inserts** that affect the transaction's result set.
# 
# -   Ensures **maximum data integrity** but reduces concurrency.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### 5\. SNAPSHOT
# 
# -   Uses **row versioning** to show a **consistent snapshot** of data at transaction start.
# 
# -   Avoids **blocking reads** and improves concurrency compared to SERIALIZABLE.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL SNAPSHOT;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### 6\. READ COMMITTED SNAPSHOT
# 
# -   Similar to **READ COMMITTED**, but uses **row versioning**.
# 
# -   Avoids **blocking**, balancing **isolation and concurrency**.
# 
# **Example:**
# ```sql
# SET TRANSACTION ISOLATION LEVEL READ COMMITTED SNAPSHOT;
# SELECT * FROM Orders;
# ```
# * * * * *
# 
# ### Interview Tip
# 
# -   Know the **difference between dirty reads, non-repeatable reads, and phantom reads**.
# 
# -   Choose isolation levels based on **application needs**:
# 
#     -   High integrity → **SERIALIZABLE** or **REPEATABLE READ**.
# 
#     -   High concurrency → **READ COMMITTED SNAPSHOT** or **SNAPSHOT**.
# 
# -   Be prepared to **explain trade-offs** between **data consistency and performance**.


# MARKDOWN ********************

# Question 51: Is a Local Temp Table Available Inside a Stored Procedure?
# -----------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   **Yes**, a local temporary table (`#table_name`) is available inside a stored procedure **if created in the same session**.
# 
# -   Local temp tables are **session-specific** and accessible to stored procedures called from the same session.
# 
# -   They are **dropped automatically** when the session ends.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Local temporary tables (`#TempTable`) exist **only for the session that created them**.
# 
# -   Any stored procedure executed **within that session** can access the temp table.
# 
# -   After the session ends, the temp table is **automatically dropped**.
# 
# * * * * *
# 
# ### 1\. Creating and Using a Local Temp Table
# 
# -   Local temp table: `#TempTable`
# 
# -   Session-specific, accessible **across procedures in the same session**.
# 
# **Example:**
# ```sql
# CREATE TABLE #TempTable(
#     ID INT,
#     Name VARCHAR(50)
# );
# 
# INSERT INTO #TempTable (ID, Name)
# VALUES (1,'John'), (2,'Jane');
# ```
# * * * * *
# 
# ### 2\. Accessing Temp Table Inside a Stored Procedure
# 
# -   The stored procedure can **select, update, or delete** data from the temp table.
# 
# **Example:**
# ```sql
# CREATE PROCEDURE YourStoredProcedure
# AS
# BEGIN
#     SELECT * FROM #TempTable;
# END;
# 
# EXEC YourStoredProcedure;
# ```
# -   Output: The procedure successfully returns the rows in `#TempTable`.
# 
# * * * * *
# 
# ### 3\. Important Notes
# 
# -   Local temp tables are **dropped automatically** when the session ends.
# 
# -   If the temp table is created **inside the stored procedure**, it exists **only for the duration of that procedure**.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Remember: `#TempTable` → session-specific, accessible by procedures in the **same session**.
# 
# -   `##GlobalTempTable` → available to **all sessions** until the last session using it ends.
# 
# -   Good question to **test your understanding of scope and temp table lifetime**.


# MARKDOWN ********************

# Question 52: Which Date Format Is Safe to Use When Passing Dates as Strings?
# ----------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   The **ISO 8601 format** is the safest for passing dates as strings.
# 
# -   **Date only:** `"YYYY-MM-DD"` → e.g., `"2023-08-05"`
# 
# -   **Date and time:** `"YYYY-MM-DDTHH:MM:SS"` → e.g., `"2023-08-05T15:30:00"`
# 
# -   Reduces **ambiguity** and is widely supported across databases and programming languages.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# Passing dates as strings can lead to **misinterpretation** due to **regional or locale settings**.
# 
# -   ISO 8601 is **unambiguous**, universally recognized, and **least affected by locale**.
# 
# -   Helps ensure **consistent parsing** across SQL Server, Oracle, MySQL, PostgreSQL, and other systems.
# 
# * * * * *
# 
# ### 1\. ISO 8601 Date Format
# 
# -   **Date only:** `"YYYY-MM-DD"`
# 
#     -   Example: `"2023-08-05"`
# 
#     -   Safe for **INSERT, UPDATE, WHERE clauses**.
# 
# -   **Timestamp with time:** `"YYYY-MM-DDTHH:MM:SS"`
# 
#     -   Example: `"2023-08-05T15:30:00"`
# 
#     -   Can include **time zone information** if supported by the database.
# 
# **Example:**
# ```sql
# -- Insert using ISO 8601 date
# INSERT INTO Orders(OrderDate)
# VALUES ('2023-08-05');
# 
# -- Insert using ISO 8601 timestamp
# INSERT INTO Orders(OrderDateTime)
# VALUES ('2023-08-05T15:30:00');
# ```
# * * * * *
# 
# ### 2\. Why Other Formats Can Be Risky
# 
# -   Formats like `"MM/DD/YYYY"` or `"DD/MM/YYYY"` may be **interpreted differently** depending on server locale.
# 
# -   Ambiguity can lead to **incorrect data insertion or query results**.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Always use **ISO 8601** when passing dates as strings in SQL queries.
# 
# -   If working with **non-ISO formats**, ensure **explicit conversion** using `CAST` or `CONVERT`.
# 
# -   ISO 8601 → **safe, consistent, and portable** across environments.


# MARKDOWN ********************

# Question 53: How Do You Suppress "Rows Affected" Messages in SQL Server?
# ------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   Use `SET NOCOUNT ON` **before** your INSERT (or other DML) statements.
# 
# -   Suppresses the **"X rows affected"** informational message.
# 
# -   Restore default behavior with `SET NOCOUNT OFF` if needed.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# By default, SQL Server returns a message showing the **number of rows affected** by `INSERT`, `UPDATE`, or `DELETE`.
# 
# -   This can **clutter output**, especially during **bulk operations** or when executing scripts programmatically.
# 
# -   `SET NOCOUNT ON` stops SQL Server from sending these messages for the session or batch.
# 
# * * * * *
# 
# ### 1\. Using SET NOCOUNT
# 
# -   Syntax:
# ```sql
# SET NOCOUNT ON;
# 
# INSERT INTO Employees (ID, Name)
# VALUES (1, 'John'), (2, 'Jane');
# 
# SET NOCOUNT OFF;  -- Optional: restore default behavior
# ```
# -   `SET NOCOUNT ON` affects **all subsequent statements** in the session or batch.
# 
# -   Makes output cleaner for **stored procedures, scripts, or automated jobs**.
# 
# * * * * *
# 
# ### 2\. Benefits
# 
# -   Reduces **network traffic** when executing multiple statements.
# 
# -   Prevents **interference with client applications** that rely on row counts.
# 
# -   Especially useful in **stored procedures** and **bulk inserts**.
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Remember: **SET NOCOUNT ON** is **SQL Server-specific**.
# 
# -   Always consider **re-enabling** with `SET NOCOUNT OFF` if the client application relies on row counts.
# 
# -   Mention its use to **improve performance and cleaner outputs** in procedural or batch operations.


# MARKDOWN ********************

# Question 54: Difference Between ANSI-89 and ANSI-92 SQL Join Syntax
# -------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   **ANSI-89**: Uses **WHERE clause** for joins, mixing **join conditions and filters**.
# 
# -   **ANSI-92**: Uses **explicit JOIN keywords** (`INNER JOIN`, `LEFT JOIN`, etc.), separating **joins from filters**.
# 
# -   ANSI-92 is **more readable, maintainable, and supports complex joins**.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# SQL has evolved in its **join syntax**:
# 
# -   **ANSI-89**: Older style, joins written in the `WHERE` clause.
# 
# -   **ANSI-92 (SQL-92)**: Introduced **explicit JOIN keywords**, improving clarity and reducing ambiguity.
# 
# * * * * *
# 
# ### 1\. ANSI-89 Syntax
# 
# -   Joins are expressed in the `WHERE` clause.
# 
# -   Can be **confusing**, especially with multiple joins and filter conditions.
# 
# **Example:**
# ```sql
# SELECT E.Name, D.DepartmentName
# FROM Employees E, Departments D
# WHERE E.DepartmentID = D.DepartmentID
#   AND D.DepartmentName = 'Sales';
# ```
# * * * * *
# 
# ### 2\. ANSI-92 Syntax
# 
# -   Uses **explicit JOINs** and **ON clause** for join conditions.
# 
# -   Separates **joining** from **filtering**.
# 
# -   Easier to **read, maintain, and optimize**.
# 
# **Example:**
# ```sql
# SELECT E.Name, D.DepartmentName
# FROM Employees E
# INNER JOIN Departments D
#     ON E.DepartmentID = D.DepartmentID
# WHERE D.DepartmentName = 'Sales';
# ```
# * * * * *
# 
# ### 3\. Key Differences
# 
# | Feature | ANSI-89 | ANSI-92 |
# | --- | --- | --- |
# | Join Specification | `WHERE` clause | `JOIN` keyword with `ON` |
# | Readability | Low for multiple joins | High, clear separation of joins and filters |
# | Support for Complex Joins | Limited | Supports INNER, LEFT, RIGHT, FULL joins |
# | Query Optimization | Less efficient | Better optimizer support in modern DBMS |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   **Always prefer ANSI-92 syntax** in modern SQL.
# 
# -   Explicit JOINs reduce **bugs, ambiguity, and maintenance overhead**.
# 
# -   Be ready to **rewrite old ANSI-89 queries using ANSI-92** during interviews.


# MARKDOWN ********************

# Question 55: Differences Between IN and EXISTS (and NOT IN, NOT EXISTS) in SQL
# ------------------------------------------------------------------------------
# 
# ### Short Answer
# 
# -   **IN**: Compares a value to a set or subquery; true if **any match** exists.
# 
# -   **EXISTS**: Checks for **existence of rows** in a subquery; true if **subquery returns any row**.
# 
# -   **NOT IN**: True if the value **does not exist** in a set or subquery.
# 
# -   **NOT EXISTS**: True if the **subquery returns no rows**.
# 
# -   **Key Difference**: IN/NOT IN compare values, EXISTS/NOT EXISTS check for row existence, often more efficient in correlated subqueries.
# 
# * * * * *
# 
# ### Detailed Explanation
# 
# -   **IN / NOT IN**: Compare a value directly against a list or subquery result.
# 
# -   **EXISTS / NOT EXISTS**: Test **whether a subquery returns any rows**, usually with a **correlated subquery**.
# 
# -   EXISTS can be **more efficient**, as it stops evaluating once a match is found.
# 
# -   Choice depends on **scenario and data structure**.
# 
# * * * * *
# 
# ### 1\. IN Operator
# 
# -   Returns rows where a value **matches any value** in a list or subquery.
# 
# **Example:**
# ```sql
# SELECT *
# FROM Employees
# WHERE DepartmentID IN (101, 102, 103);
# ```
# * * * * *
# 
# ### 2\. EXISTS Operator
# 
# -   Returns true if a **subquery returns any rows**.
# 
# **Example:**
# ```sql
# SELECT *
# FROM Orders O
# WHERE EXISTS (
#     SELECT 1
#     FROM Customers C
#     WHERE C.CustomerID = O.CustomerID
# );
# ```
# * * * * *
# 
# ### 3\. NOT IN Operator
# 
# -   Returns rows where a value **does not match any value** in a list or subquery.
# 
# **Example:**
# ```sql
# SELECT *
# FROM Students
# WHERE Age NOT IN (18, 19, 20);
# ```
# * * * * *
# 
# ### 4\. NOT EXISTS Operator
# 
# -   Returns true if the **subquery returns no rows**.
# 
# **Example:**
# ```sql
# SELECT *
# FROM Products P
# WHERE NOT EXISTS (
#     SELECT 1
#     FROM Orders O
#     WHERE O.ProductID = P.ProductID
# );
# ```
# * * * * *
# 
# ### Key Differences
# 
# | Feature | IN / NOT IN | EXISTS / NOT EXISTS |
# | --- | --- | --- |
# | Comparison Type | Direct value comparison | Row existence check |
# | Typical Use | List of values or subquery | Correlated subqueries |
# | Efficiency | Can be slower with large subquery results | Stops at first match → usually more efficient |
# | NULL Handling | NULL in subquery can cause unexpected results | Handles NULLs more reliably |
# 
# * * * * *
# 
# ### Interview Tip
# 
# -   Use **IN / NOT IN** for **small lists** or simple subqueries.
# 
# -   Use **EXISTS / NOT EXISTS** for **correlated subqueries** or large datasets.
# 
# -   Be aware of **NULLs**, which can affect NOT IN results unexpectedly.
# 
# -   Highlight understanding of **performance differences** during interviews.

