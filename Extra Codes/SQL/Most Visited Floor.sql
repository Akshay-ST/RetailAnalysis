CREATE TABLE building_visits (
visit_date_time TIMESTAMP,
visiter_name VARCHAR(15),
visited_floor INTEGER );

INSERT INTO building_visits VALUES (
'2023-12-28 09:00', 'Arjun Patel', 5),
('2023-12-28 10:30', 'Priya Gupta', 8),
('2023-12-28 11:15', 'Arjun Patel', 10),
('2023-12-28 14:00', 'Ananya Singh', 3),
('2023-12-28 15:45', 'Priya Gupta', 8),
('2023-12-28 16:30', 'Arjun Patel', 5),
('2023-12-28 17:20', 'Ananya Singh', 2),
('2023-12-28 18:10', 'Priya Gupta', 6),
('2023-12-28 18:45', 'Arjun Patel', 1),
('2023-12-28 19:30', 'Ananya Singh', 2);

WITH cte1 AS (
SELECT visiter_name
 , COUNT(1) AS total_visit
FROM building_visits
GROUP BY 1), 
cte2 AS (
    SELECT visiter_name,
    visited_floor
    FROM (
    SELECT visiter_name,
    visited_floor,
    RANK() OVER(PARTITION BY visiter_name ORDER BY COUNT(visited_floor) DESC) AS rnk
    FROM building_visits
    GROUP BY 1, 2 ) A
    WHERE rnk = 1
)
SELECT cte2.visiter_name,
 cte1.total_visit,
 cte2.visited_floor AS most_visit_floor
FROM cte1
INNER JOIN cte2 ON cte1.visiter_name = cte2.visiter_name;

WITH cte AS (
SELECT visiter_name,
 visited_floor,
 COUNT(1) AS total_visit,
 RANK() OVER(PARTITION BY visiter_name ORDER BY COUNT(visited_floor) DESC) AS most_visited_floor
FROM building_visits
GROUP BY 1, 2)
SELECT visiter_name,
 SUM(total_visit) AS total_visit,
 MAX(CASE WHEN most_visited_floor = 1 THEN visited_floor END) AS most_visit_floor
FROM cte
GROUP BY 1;

