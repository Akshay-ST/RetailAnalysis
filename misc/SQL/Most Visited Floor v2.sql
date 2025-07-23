CREATE TABLE entries(
    name varchar(20),
    address varchar(20),
    Email varchar(20),
    floor int,
    resource varchar(20)
);

INSERT INTO entries VALUES('A','Bangalore','A@gmail.com',1,'CPU');
INSERT INTO entries VALUES('A','Bangalore','A1@gmail.com',1,'CPU');
INSERT INTO entries VALUES('A','Bangalore','A2@gmail.com',2,'Desktop');
INSERT INTO entries VALUES('B','Bangalore','B@gmail.com',2,'Desktop');
INSERT INTO entries VALUES('B','Bangalore','B1@gmail.com',2,'Desktop');
INSERT INTO entries VALUES('B','Bangalore','B2@gmail.com',1,'Moniter');

WITH distinct_resources AS (
    SELECT DISTINCT name, resource FROM entries
),
agg_resources AS (
    SELECT name, GROUP_CONCAT(resource SEPARATOR ', ') AS agg_res 
    FROM distinct_resources 
    GROUP BY name
),
floor_visit AS (
    SELECT name, floor, COUNT(1) AS no_of_times_visited, 
           RANK() OVER (PARTITION BY name ORDER BY COUNT(1) desc) AS rnk
    FROM entries 
    GROUP BY name, floor
)
SELECT fv.name, fv.floor AS most_visited_floor, ar.agg_res 
FROM floor_visit fv 
INNER JOIN agg_resources ar 
ON fv.name = ar.name 
WHERE fv.rnk = 1;