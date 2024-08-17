CREATE TABLE Akshay.Employee (
	emp_id int,
	name varchar(20),
	dept varchar(20),
	month varchar(20), 
	salary long
)
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci;

insert into Employee values
(1,'A','dept-a','Jan',3000);

insert into Employee values
(2,'B','dept-a','Jan',3500),
(3,'c','dept-a','Jan',4500),
(4,'D','dept-b','Jan',3000),
(5,'E','dept-b','Jan',3500);

insert into Employee values
(6,'F','dept-b','Jan',4000),
(7,'G','dept-b','Jan',4500);

select * from Employee;

/*
 * Find employees whose monthly salary is above 
 * the average salary of their department:
 */

select 
	emp.emp_Id,
	emp.Salary

from Employee emp
join (
	select Dept, avg(salary) as dept_avg_sal
	from Employee
	group by Dept
) avg_sal on avg_sal.dept = emp.dept
	
where emp.salary > avg_sal.dept_avg_sal;

/*
 * List the top 3 highest paid employees in each department for the latest month:
 */

SELECT 
	Dept,
	Emp_id,
	Name,
	Salary,
	rnk
from (
	select 
		Dept,
		Emp_id,
		Name,
		Salary,
		DENSE_RANK() OVER (PARTITION BY Dept ORDER BY salary DESC) as rnk 
		
	from Employee
	where month = 'Jan'
) emp
where rnk <= 3
ORDER BY Dept, Salary desc;





