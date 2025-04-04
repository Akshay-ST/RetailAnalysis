CREATE TABLE products (
    PRODUCT_ID INT,
    PRICE_DATE DATE,
    PRICE INT
);

INSERT INTO products (PRODUCT_ID, PRICE_DATE, PRICE) VALUES
(100, '2024-01-01', 150),
(100, '2024-01-21', 170),
(100, '2024-02-01', 190),
(101, '2024-01-01', 1000),
(101, '2024-01-27', 1200),
(101, '2024-02-05', 1250);


CREATE TABLE orders (
    ORDER_ID INT,
    ORDER_DATE DATE,
    PRODUCT_ID INT
);

INSERT INTO orders (ORDER_ID, ORDER_DATE, PRODUCT_ID) VALUES
(1, '2024-01-05', 100),
(2, '2024-01-21', 100),
(3, '2024-02-20', 100),
(4, '2024-01-07', 101),
(5, '2024-02-04', 101),
(6, '2024-02-05', 101);

WITH CTE AS (
    SELECT *,
           DATE_ADD(
               LEAD(PRICE_DATE, 1, '9999-12-31') OVER (PARTITION BY PRODUCT_ID ORDER BY PRICE_DATE),
               INTERVAL -1 DAY
           ) AS PRICE_END_DATE
    FROM products
)
SELECT 
    c.PRODUCT_ID,
    SUM(c.PRICE) AS total_sales
FROM CTE c
JOIN orders o 
    ON c.PRODUCT_ID = o.PRODUCT_ID
   AND o.ORDER_DATE BETWEEN c.PRICE_DATE AND c.PRICE_END_DATE
GROUP BY c.PRODUCT_ID
ORDER BY c.PRODUCT_ID;













