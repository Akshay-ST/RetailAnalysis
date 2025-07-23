create  table lifts(
id int,
capacity_kg int
);

insert into lifts values 
(1,300),
(2,350)
;


create table lift_passengers(
passenger_name VARCHAR(20),
weight_kg int,
lift_id int
);

insert into lift_passengers values 
('Rahul',85,1),
('Adarsh',73,1),
('Riti',95,1),
('Dheeraj',80,1),
('Vimal',83,2),
('Neha',77,2),
('Priti',73,2),
('Himanshi',85,2)
;


with CTE as (
select 
	l.id,
	lp.passenger_name,
	lp.weight_kg,
	l.capacity_kg,
	sum(lp.weight_kg) OVER (PARTITION BY l.id 
				ORDER BY lp.weight_kg
				ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
				) as running_sum
	from lifts l
	join lift_passengers lp on l.id = lp.lift_id
)
select 
	id,
	GROUP_CONCAT(DISTINCT passenger_name
		     ORDER BY weight_kg SEPARATOR ',') AS passenger_List
FROM CTE
WHERE running_sum < capacity_kg
GROUP BY ID
ORDER BY ID;



