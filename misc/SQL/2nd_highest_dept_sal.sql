/*
id  name    salary  department
1  Alice    80000   IT
2  Bob      60000   IT
3  Charlie  90000   IT
4  David    85000   HR
5  Eva      80000   HR
6  Frank    75000   HR
7  George   70000   Finance
*/

WITH ranked AS (
  SELECT 
    department,
    name,
    salary,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rnk
  FROM Employee
),
depts AS (
  SELECT DISTINCT department FROM Employee
)
SELECT 
  d.department,
  r.name,
  r.salary
FROM depts d
LEFT JOIN ranked r ON d.department = r.department AND r.rnk = 2;
