Use Akshay;


CREATE TABLE employees_cte (
 id INT PRIMARY KEY,
 name VARCHAR(100),
 department VARCHAR(100),
 manager_id INT,
 salary DECIMAL(10,2),
 hire_date DATE
);

INSERT INTO employees_cte (id, name, department, manager_id, salary, hire_date)
VALUES
(1, 'Alice', 'HR', NULL, 70000, '2015-06-23'),
(2, 'Bob', 'IT', 1, 90000, '2016-09-17'),
(3, 'Charlie', 'Finance', 1, 80000, '2017-02-01'),
(4, 'David', 'IT', 2, 75000, '2018-07-11'),
(5, 'Eve', 'Finance', 3, 72000, '2019-04-30'),
(6, 'Frank', 'HR', 1, 68000, '2020-01-15'),
(7, 'Grace', 'IT', 2, 85000, '2021-03-22'),
(8, 'Heidi', 'Finance', 3, 78000, '2022-05-10'),
(9, 'Ivan', 'HR', 1, 69000, '2023-08-05'),
(10, 'Judy', 'IT', 2, 92000, '2024-02-14'),
(11, 'Karl', 'Finance', 3, 77000, '2024-03-01'),
(12, 'Leo', 'HR', 1, 71000, '2024-04-20'),
(13, 'Mallory', 'IT', 2, 88000, '2024-05-15'),
(14, 'Nina', 'Finance', 3, 76000, '2024-06-10'),
(15, 'Oscar', 'HR', 1, 72000, '2024-07-01'),
(16, 'Peggy', 'IT', 2, 93000, '2024-08-20'),
(17, 'Quentin', 'Finance', 3, 79000, '2024-09-05'),
(18, 'Ruth', 'HR', 1, 73000, '2024-10-15'),
(19, 'Steve', 'IT', 2, 94000, '2024-11-01'),
(20, 'Trudy', 'Finance', 3, 80000, '2024-12-10');

-- Recursive CTEs
-- 1. How do you retrieve employees and their hierarchical managers recursively?

WITH RECURSIVE EmployeeHierarchy AS (
    -- ANCHOR QUERY
    SELECT 
        id, 
        name, 
        manager_id, 
        1 AS level
    FROM employees_cte 
    WHERE manager_id IS NULL

    UNION ALL
    
    -- RECURSIVE QUERY
    SELECT 
        e.id, 
        e.name, 
        e.manager_id, 
        eh.level + 1
    FROM employees_cte e 
    JOIN EmployeeHierarchy eh 
        ON e.manager_id = eh.id
)
SELECT * FROM EmployeeHierarchy ORDER BY level;


-- 2. How do you calculate the number of levels in an organizational hierarchy?

WITH RECURSIVE Hierarchy AS (
    SELECT 
        id, 
        name, 
        manager_id, 1 AS level 
    FROM employees_cte
    WHERE manager_id IS NULL
    UNION ALL
    SELECT 
        e.id, 
        e.name, 
        e.manager_id, 
        h.level + 1
    FROM employees_cte e 
    JOIN Hierarchy h 
        ON e.manager_id = h.id
)
SELECT MAX(level) AS max_levels FROM Hierarchy;

-- 3. How can you calculate cumulative salary within each department?
WITH RunningSalary AS (
    SELECT 
        department, 
        name, 
        salary,
        SUM(salary) OVER (PARTITION BY department ORDER BY hire_date) AS running_total
FROM employees_cte
)
SELECT * FROM RunningSalary;


-- 4. How do you find the most recent hire in each department?
WITH LatestHire AS (
 SELECT 
    department, 
    name, 
    hire_date,
    RANK() OVER (PARTITION BY department ORDER BY hire_date DESC) AS rnk
 FROM employees_cte
)
SELECT * FROM LatestHire WHERE rnk = 1;

-- 5. How can you identify employees without direct reports?
WITH EmployeeReport AS (
 SELECT 
    e1.id, 
    e1.name, 
    COUNT(e2.id) AS report_count
 FROM employees_cte e1 
 LEFT JOIN employees_cte e2 ON e1.id = e2.manager_id
 GROUP BY e1.id, e1.name
)
SELECT * FROM EmployeeReport WHERE report_count = 0; 


-- Ranking & Aggregation
-- 6. How do you rank employees based on salary within each department?
WITH SalaryRanking AS (
SELECT 
    id, 
    name,
    department, 
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
FROM employees_cte
)
SELECT * FROM SalaryRanking WHERE rnk <= 3;