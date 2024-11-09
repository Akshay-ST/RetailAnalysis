CREATE TABLE employee_in_out (
    EMP_ID INT,
    ACTION VARCHAR(5),
    CREATED_AT DATETIME
);

INSERT INTO employee_in_out (EMP_ID, ACTION, CREATED_AT) VALUES
(1, 'in', '2019-04-01 12:00:00'),
(1, 'out', '2019-04-01 15:00:00'),
(1, 'in', '2019-04-01 17:00:00'),
(1, 'out', '2019-04-01 21:00:00'),
(2, 'in', '2019-04-01 10:00:00'),
(2, 'out', '2019-04-01 13:00:00'),
(3, 'in', '2019-04-01 19:00:00'),
(3, 'out', '2019-04-02 05:00:00'),
(4, 'in', '2019-04-01 18:00:00'),
(4, 'out', '2019-04-02 20:00:00'),
(5, 'in', '2019-04-02 09:00:00'),
(5, 'out', '2019-04-02 11:00:00'),
(6, 'in', '2019-04-02 10:00:00'),
(6, 'out', '2019-04-02 16:00:00');


set @max_timestamp = (select max(created_at) from employee_in_out);
set @min_timestamp = (select min(created_at) from employee_in_out);

with cte1 as (
    select *,
    lead(created_at) over (partition by emp_id order by created_at) as next_created_at
    from employee_in_out
),
considered_time as (
    select emp_id,
    case when created_at < '2019-04-01 14:00:00' then '2019-04-01 14:00:00' else created_at end as in_time,
    case when created_at > '2019-04-02 10:00:00' then '2019-04-02 10:00:00' else next_created_at end as out_time
    from cte1
    where action='in'
)
select emp_id,
ROUND(sum(case when in_time > out_time then 0
                else timestampdiff(minute,in_time,out_time) end) , 1)
    as time_spent_in_mins
from considered_time
GROUP BY emp_id
order by emp_id;

-------------------------------------

WITH in_out_times AS (
    SELECT 
        EMP_ID,
        ACTION,
        CREATED_AT,
        LEAD(CREATED_AT) OVER (PARTITION BY EMP_ID ORDER BY CREATED_AT) AS NEXT_TIME
    FROM employee_in_out
),
time_spent AS (
    SELECT
        EMP_ID,
        CASE
            WHEN ACTION = 'in' THEN TIMESTAMPDIFF(MINUTE, CREATED_AT, NEXT_TIME)
            ELSE 0
        END AS TIME_SPENT_IN_MINS
    FROM in_out_times
)
SELECT
    EMP_ID,
    SUM(TIME_SPENT_IN_MINS) AS TIME_SPENT_IN_MINS
FROM time_spent
GROUP BY EMP_ID
ORDER BY EMP_ID;