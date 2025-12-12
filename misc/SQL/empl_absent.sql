use Akshay;

show databases;
-- Create table (if needed)
CREATE TABLE employee_attendance (
    emp_id VARCHAR(10),
    date DATE,
    status VARCHAR(10)
);

-- INSERT sample data with consecutive absences
INSERT INTO employee_attendance VALUES
-- E001: Absent 3 consecutive days (Jan 15-17)
('E001', '2025-01-14', 'present'),
('E001', '2025-01-15', 'absent'),
('E001', '2025-01-16', 'absent'),
('E001', '2025-01-17', 'absent'),
('E001', '2025-01-18', 'present'),
-- E002: Absent 4 consecutive days (Jan 20-23)
('E002', '2025-01-19', 'present'),
('E002', '2025-01-20', 'absent'),
('E002', '2025-01-21', 'absent'),
('E002', '2025-01-22', 'absent'),
('E002', '2025-01-23', 'absent'),
-- E003: Only 2 consecutive (won't show in results)
('E003', '2025-01-24', 'present'),
('E003', '2025-01-25', 'absent'),
('E003', '2025-01-26', 'absent'),
('E003', '2025-01-27', 'present'),
-- E004: Absent 5 consecutive days (Jan 28-Feb 1)
('E004', '2025-01-27', 'present'),
('E004', '2025-01-28', 'absent'),
('E004', '2025-01-29', 'absent'),
('E004', '2025-01-30', 'absent'),
('E004', '2025-01-31', 'absent'),
('E004', '2025-02-01', 'absent'),
-- Regular attendance employees
('E005', '2025-01-28', 'present'),
('E005', '2025-01-29', 'present'),
('E006', '2025-01-30', 'present'),
('E006', '2025-01-31', 'absent'),
('E007', '2025-02-01', 'present');


-----------------------------
WITH consecutive_absences AS (
  SELECT 
    emp_id,
    date,
    status,
    LAG(status, 1) OVER (PARTITION BY emp_id ORDER BY date) AS prev_status,
    SUM(CASE WHEN status = 'absent' THEN 1 ELSE 0 END) 
        OVER (PARTITION BY emp_id ORDER BY date 
              ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS absent_streak
  FROM employee_attendance
),
consecutive_3_plus AS (
  SELECT 
    emp_id,
    date,
    absent_streak
  FROM consecutive_absences
  WHERE absent_streak >= 3
    AND status = 'absent'
    AND COALESCE(prev_status, 'present') != 'present'
)
SELECT DISTINCT emp_id, date AS first_absent_date
FROM consecutive_3_plus
ORDER BY emp_id, date;



-----------------------------
WITH absence_streaks AS (
  SELECT 
    emp_id,
    date,
    status,
    SUM(CASE WHEN status = 'absent' THEN 1 ELSE 0 END) 
      OVER (PARTITION BY emp_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING) AS total_absences_so_far,
    SUM(CASE WHEN status = 'present' THEN 1 ELSE 0 END) 
      OVER (PARTITION BY emp_id 
            ORDER BY date 
            ROWS UNBOUNDED PRECEDING) AS presents_so_far
  FROM employee_attendance
)
SELECT 
  emp_id,
  date,
  status
FROM absence_streaks
WHERE status = 'absent'
  AND (total_absences_so_far - COALESCE(presents_so_far, 0)) >= 3
ORDER BY emp_id, date;




-------------------
WITH only_absent AS (
    SELECT
        emp_id,
        date,
        status
    FROM employee_attendance
    WHERE status = 'absent'
),
-- Identify consecutive-date groups within absences
absent_with_grp AS (
    SELECT
        emp_id,
        date,
        status,
        DATE_ADD(
            day,
            - ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY date),
            date
        ) AS grp_key
        -- In Postgres use: date - (ROW_NUMBER() OVER (...) * INTERVAL '1 day')
        -- In MySQL: DATE_SUB(date, INTERVAL ROW_NUMBER() OVER (...) DAY)
    FROM only_absent
),
grouped AS (
    SELECT
        emp_id,
        grp_key,
        COUNT(*) AS streak_len
    FROM absent_with_grp
    GROUP BY emp_id, grp_key
    HAVING COUNT(*) >= 3
)
SELECT
    a.emp_id,
    a.date,
    a.status
FROM absent_with_grp a
JOIN grouped g
  ON a.emp_id = g.emp_id
 AND a.grp_key = g.grp_key
ORDER BY a.emp_id, a.date;
