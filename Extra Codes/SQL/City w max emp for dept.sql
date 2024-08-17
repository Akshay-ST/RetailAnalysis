-- Find city with maximum number of employees for each department

create table Employee_city(
Emp_id int,
City VARCHAR(20),
dept VARCHAR(20)
);

INSERT INTO Employee_city (Emp_id, City, dept) VALUES
(1, 'New York', 'HR'),
(2, 'New York', 'Finance'),
(3, 'New York', 'IT'),
(4, 'Los Angeles', 'HR'),
(5, 'Los Angeles', 'Finance'),
(6, 'Los Angeles', 'IT'),
(7, 'Chicago', 'HR'),
(8, 'Chicago', 'Finance'),
(9, 'Chicago', 'IT'),
(10, 'New York', 'HR'),
(11, 'New York', 'Finance'),
(12, 'New York', 'IT'),
(13, 'Los Angeles', 'HR'),
(14, 'Los Angeles', 'Finance'),
(15, 'Los Angeles', 'IT'),
(16, 'Chicago', 'HR'),
(17, 'Chicago', 'Finance'),
(18, 'Chicago', 'IT'),
(19, 'New York', 'HR'),
(20, 'New York', 'Finance'),
(21, 'New York', 'IT'),
(22, 'Los Angeles', 'HR'),
(23, 'Los Angeles', 'Finance'),
(24, 'Los Angeles', 'IT'),
(25, 'Chicago', 'HR'),
(26, 'Chicago', 'Finance'),
(27, 'Chicago', 'IT'),
(28, 'New York', 'HR'),
(29, 'New York', 'Finance'),
(30, 'New York', 'IT');

select * from Employee_city;



select 
	dept, city, head_count
from (
	select dept, city, count(Emp_id) head_count,
			DENSE_RANK() OVER (PARTITION BY dept ORDER BY Count(Emp_id) desc) as rnk
	from Employee_city
	group by 1,2
	order by 1,2
) emp
where rnk = 1;



