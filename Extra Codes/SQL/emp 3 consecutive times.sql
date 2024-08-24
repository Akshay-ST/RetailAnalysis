/*
Find employ who has  occurred only consecutively 3 times
*/



CREATE TABLE empl_1 (
    emp_name VARCHAR(50),
    year BIGINT
);

drop table empl_1;

INSERT INTO empl_1 (emp_name, year) VALUES
('emp 1', 2000),
('emp 1', 2001),
('emp 1', 2002),
('emp 2', 2001),
('emp 2', 2002),
('emp 3', 2000),
('emp 3', 2001),
('emp 3', 2003),
('emp 4', 2000),
('emp 4', 2001),
('emp 4', 2002),
('emp 4', 2003);

WITH CTE AS (
    SELECT 
        emp_name,
        year,
        ROW_NUMBER() OVER (PARTITION BY emp_name ORDER BY year) AS rn1,
        year - ROW_NUMBER() OVER (PARTITION BY emp_name ORDER BY year) AS rn2
    FROM empl_1
),
GroupedYears AS (
    SELECT 
        emp_name,
        COUNT(*) AS consecutive_years
    FROM CTE
    GROUP BY emp_name, rn2
    HAVING COUNT(*) = 3
)
SELECT 
    emp_name
FROM GroupedYears
GROUP BY emp_name
HAVING COUNT(*) = 1;
