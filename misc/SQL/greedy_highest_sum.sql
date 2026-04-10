/*
maximum items that can be bought for 100 
input 
+------------+-------+----------+ 
| name | price | quantity | 
+------------+-------+----------+ 
| Pencil | 3 | 30 | 
| Eraser | 5 | 3 | 
| Notebook | 5 | 3 | 
| Pen | 6 | 20 | 
+------------+-------+----------+ 

ouptput: 
+----------+ 
| quantity | 
+----------+ 
| 32 | 
+----------+


*/

use Akshay;

Create table items (
    name varchar(20),
    price int,
    quantity int
);

insert into items values ('Pencil', 3, 30);
insert into items values ('Eraser', 5, 3);
insert into items values ('Notebook', 5, 3);
insert into items values ('Pen', 6, 20);



--Solution 1:
WITH expanded AS (
    -- Expand each item based on quantity
    SELECT name, price
    FROM items
    JOIN generate_series(1, quantity) AS g(n) ON TRUE
),
ordered AS (
    SELECT 
        price,
        SUM(price) OVER (ORDER BY price ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost
    FROM expanded
)
SELECT COUNT(*) AS quantity
FROM ordered
WHERE running_cost <= 100;

--Solution 2:
WITH RECURSIVE expanded AS (
    SELECT name, price, quantity, 1 AS cnt
    FROM items
    UNION ALL
    SELECT name, price, quantity, cnt + 1
    FROM expanded
    WHERE cnt < quantity
),
ordered AS (
    SELECT 
        price,
        SUM(price) OVER (ORDER BY price) AS running_cost
    FROM expanded
)
SELECT COUNT(*) AS quantity
FROM ordered
WHERE running_cost <= 100;


#Solution 3:

WITH ordered AS (
    SELECT *,
           SUM(price * quantity) OVER (ORDER BY price) AS cumulative_cost
    FROM items
),
calc AS (
    SELECT *,
        LAG(cumulative_cost, 1, 0) OVER (ORDER BY price) AS prev_cost
    FROM ordered
)
SELECT SUM(
    CASE 
        WHEN cumulative_cost <= 100 THEN quantity
        WHEN prev_cost < 100 THEN FLOOR((100 - prev_cost) / price)
        ELSE 0
    END
) AS quantity
FROM calc;