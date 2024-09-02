CREATE TABLE employee_logins (
    EMP_ID INT,
    LOGIN DATETIME,
    LOGOUT DATETIME
);

INSERT INTO employee_logins (EMP_ID, LOGIN, LOGOUT) VALUES
(100, '2024-02-19 09:15:00', '2024-02-19 18:20:00'),
(100, '2024-02-20 09:05:00', '2024-02-20 17:00:00'),
(100, '2024-02-21 09:00:00', '2024-02-21 17:10:00'),
(100, '2024-02-22 10:00:00', '2024-02-22 16:55:00'),
(100, '2024-02-23 09:30:00', '2024-02-23 17:10:00'),
(200, '2024-02-19 09:10:00', '2024-02-19 16:30:00'),
(200, '2024-02-20 09:00:00', '2024-02-20 16:00:00'),
(200, '2024-02-21 09:15:00', '2024-02-21 17:05:00'),
(200, '2024-02-22 11:00:00', '2024-02-22 17:00:00'),
(200, '2024-02-23 09:30:00', '2024-02-23 16:30:00'),
(300, '2024-02-19 09:30:00', '2024-02-19 16:20:00'),
(300, '2024-02-20 09:15:00', '2024-02-20 16:30:00'),
(300, '2024-02-22 11:00:00', '2024-02-22 17:00:00'),
(300, '2024-02-23 09:30:00', '2024-02-23 16:30:00');


-- with emp_hrs as (
--     select 
--     emp_id,
--     login,
--     logout,
--     date(LOGIN) as days_worked, 
--     TIMESTAMPDIFF(HOUR,login, logout) as hrs_worked,
--     ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY date(LOGIN)) as rn
--     from employee_logins
-- )
-- select `EMP_ID`,
--         case 
--             when max(hrs_worked) >= 8 and max(rn) >= 3 then 1
--             when max(hrs_worked) >= 10 and max(rn) >= 2 then 2
--             when ((max(hrs_worked) >= 8  and max(rn) >= 3) OR
--                   (max(hrs_worked) >= 10 and max(rn) >= 2))
--             then 'both'
--             end as criterion
-- from emp_hrs
-- GROUP BY `EMP_ID`
-- order by `EMP_ID`

with emp as (
    select 
        emp_id,
        count(case when TIMESTAMPDIFF(minute,login,logout)/60.0 >= 8 then '8+' end) as day_8,
        count(case when TIMESTAMPDIFF(minute,login,logout)/60.0 >= 10 then '10+' end) as day_10
    from employee_logins
    where TIMESTAMPDIFF(minute, login, logout)/60.0 > 8
    GROUP BY EMP_ID
)
select EMP_ID,
        case 
            when day_8 >= 3 and day_10 >= 2 then 'both'
            when day_8 >= 3 then '1'
            when day_10 >= 2 then '2'
            end as criterion
from emp
