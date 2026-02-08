
CREATE TABLE journeys (
    customer_id     INT,
    from_city       VARCHAR(50),
    to_city         VARCHAR(50),
    journey_order   INT
);


INSERT INTO journeys (customer_id, from_city, to_city, journey_order) VALUES
(1, 'Delhi',      'Hyderabad',  1),
(1, 'Hyderabad',  'Chennai',    2),
(1, 'Chennai',    'Coimbatore', 3),
(2, 'Mumbai',     'Pune',       1),
(2, 'Pune',       'Bangalore',  2);


WITH ranked_journeys AS (
    SELECT
        customer_id,
        from_city,
        to_city,
        journey_order,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY journey_order) AS rn_start,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY journey_order DESC) AS rn_end
    FROM journeys
)
SELECT
    customer_id,
    MAX(CASE WHEN rn_start = 1 THEN from_city END) AS from_city,
    MAX(CASE WHEN rn_end = 1 THEN to_city END) AS to_city
FROM ranked_journeys
GROUP BY customer_id;


SELECT
    j1.customer_id,
    j1.from_city,
    j2.to_city
FROM journeys j1
JOIN journeys j2
  ON j1.customer_id = j2.customer_id
WHERE j1.journey_order = (
        SELECT MIN(journey_order)
        FROM journeys
        WHERE customer_id = j1.customer_id
      )
  AND j2.journey_order = (
        SELECT MAX(journey_order)
        FROM journeys
        WHERE customer_id = j2.customer_id
      );


WITH cte AS (
 SELECT 
 customer_id,
 MIN(journey_order) AS min_id,
 MAX(journey_order) AS max_id
 FROM journeys
 GROUP BY customer_id
)
SELECT
 c.customer_id,
 MAX(CASE WHEN c.journey_order = t.min_id THEN c.from_city END) AS from_city,
 MAX(CASE WHEN c.journey_order = t.max_id THEN c.to_city END) AS to_city
FROM journeys c
JOIN cte t
 ON c.customer_id = t.customer_id
GROUP BY c.customer_id;


SELECT DISTINCT
    customer_id,
    FIRST_VALUE(from_city) OVER (
        PARTITION BY customer_id
        ORDER BY journey_order
    ) AS from_city,
    LAST_VALUE(to_city) OVER (
        PARTITION BY customer_id
        ORDER BY journey_order
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS to_city
FROM journeys;
