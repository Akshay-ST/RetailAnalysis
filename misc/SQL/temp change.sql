-- Active: 1723900249654@@127.0.0.1@3306@Akshay
# Write your MySQL query statement below

SELECT id
FROM (
    SELECT
        id,
        temperature,
        recordDate,
        LAG(temperature) OVER (ORDER BY recordDate) AS prev_temp,
        LAG(recordDate)  OVER (ORDER BY recordDate) AS prev_date
    FROM Weather
) t
WHERE prev_date = DATE_ADD(recordDate, INTERVAL -1 DAY)
  AND temperature > prev_temp;


--------------------------------------
SELECT w2.id
FROM Weather w1
JOIN Weather w2
  ON w2.recordDate = DATE_ADD(w1.recordDate, INTERVAL 1 DAY)
WHERE w2.temperature > w1.temperature;


| id | recordDate | temperature |
| -- | ---------- | ----------- |
| 1  | 2000-12-16 | 3           |
| 2  | 2000-12-15 | -1          |

