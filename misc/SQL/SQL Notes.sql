Root Pass = 123456789
/*
https://www.geeksforgeeks.org/frame-clause-in-sql/

FRAME clause is used with window/analytic functions in SQL. 
Whenever we use a window function, it creates a ‘window’ or a ‘partition’ depending upon the column mentioned after the ‘partition by’ clause in the ‘over’ clause. 
And then it applies that window function to each of those partitions and inside these partitions, we can create a subset of records using the FRAME clause.
Therefore, the FRAME clause specifies a subset.

Suppose we have to display two extra columns which will display the most expensive  (‘Exp_Product’) and the least expensive (‘Che_Product’) product of a particular category with every row of the table using ‘first_value’ and ‘last_value’ window functions.
*/

Select * ,
first_value(Product_Name) over (partition by Category order by Price desc) as Exp_Product,
last_value(Product_Name) over (partition by Category order by Price desc) as Che_Product
from STATIONERY;

--^^Output wrong
--DEFAULT FRAME is a  ‘range between unbounded preceding and current row’

Select *,
first_value(Product_Name) over (partition by Category order by Price desc 
				range between unbounded preceding and current row) as Exp_Product,
last_value(Product_Name) over (partition by Category order by Price desc 
				range between unbounded preceding and current row) as Che_Product
from STATIONERY;

--^^Output wrong, gives same output


Select *,
first_value(Product_Name) over (partition by Category order by Price desc 
				range between unbounded preceding and unbounded following) as Exp_Product,
last_value(Product_Name) over(partition by Category order by Price desc 
				range between unbounded preceding and unbounded following) as Che_Product 
from STATIONERY;

==
---BEFORE
select user_id,
	MAX(Case when rn=1 THEN source END) as first_source,
	MAX(CASE when rn_last=1 THEN price END) as last_price
FROM (
	select user_id,
		source,
		price,
	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time) as rn,
	ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_time DESC) as rn_last,
	FROM events) as t
group by user_id

--AFTER

select DISTINCT user_id,
	FIRST_VALUE(source) OVER (PARTITION BY user_id ORDER BY event_time ROWS 
				BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first_source,
	LAST_VALUE(price) OVER (PARTITION BY user_id ORDER BY event_time ROWS 
				BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_price
FROM events
	
--------------------------------------
--similar example
/*
Given a table with 'brands' and missing 'category' values, can you craft an SQL query that fills those gaps with the last non-null category?
'category','brand'
('chocolates',	'5-star')
,(None,		'dairy milk')
,(None,		'perk')
,(None,		'eclair')
,('Biscuits',	'britannia')
,(None,		'good day')
,(None,		'boost')

--OP
category	brand_name
chocolates	5-star
chocolates	dairy milk
chocolates	perk
chocolates	eclair
Biscuits	britannia
Biscuits	good day
Biscuits	boost
*/
SELECT 
 COALESCE(category, LAG(category IGNORE NULLS) OVER (ORDER BY monotonically_increasing_id())) AS category,
 brand_name
FROM (
 SELECT *,
 ROW_NUMBER() OVER (ORDER BY monotonically_increasing_id()) AS order_id
 FROM my_table) t

ORDER BY order_id;

----or
with cte1 as (
 select *, row_number() over(order by (select null)) as id -- 𝐠𝐫𝐞𝐚𝐭 𝐭𝐫𝐢𝐜𝐤 𝐭𝐨 𝐠𝐞𝐧𝐞𝐫𝐚𝐭𝐞 𝐫𝐨𝐰 𝐧𝐮𝐦𝐛𝐞𝐫
 from brands
), 
/*
category	brand_name		id
chocolates	5-star			1
NULL		dairy milk		2
NULL		perk			3
NULL		eclair			4
Biscuits	britannia		5
NULL		good day		6
NULL		boost			7
*/

cte2 as (
 select *, lead(id) over() - 1 as prev_id
 from cte1
 where category is not null
)
/*
category	brand_name	id	prev_id
chocolates	5-star		1	4
Biscuits	britannia	5	NULL
*/


 select c2.category, c1.brand_name
 from cte1 c1
 inner join cte2 c2 on c1.id between c2.id and prev_id or (c1.id between c2.id and prev_id is null)
 
 
----------------------------------------------------------------
--NVL
--replcases empty value

--NVL2
--Returns one of two values based on whether a specified expression evaluates to NULL or NOT NULL.
--NVL2 ( expression, not_null_return_value, null_return_value )

select user_id,
	NVL2(phone, phone, email) as contact_info
from users
limit 10

--assuming col 1 == NULL
select decode(column1, null, 1234, '2345');--returns INTEGER
select nvl2(column1, '2345', 1234);--returns VARCHAR

--more on decode
SELECT user_id,
	CASE
		WHEN dial_code = '+966' THEN 'KSA'
		when dial_code = '+971' THEN 'UAE'
		ELSE 'Other'
	END as registration_country
FROM User

select user_id,
	DECODE(dial_code, '+966', 'KSA', '+971', 'UAE', 'Other') as registration_country
FROM user

---------------------------------------------------------
--LISTAGG
--https://www.geeksforgeeks.org/sql-listagg/

select student_name,
	LISTAGG(subject, ',') WITHIN GROUP (ORDER BY grade DESC) as concatenated_subjects
FROM table
group by student_name

/*
LISTAGG (measure_expr [, 'delimiter']) WITHIN GROUP (order_by_clause) [OVER query_partition_clause]
measure_expr : The column or expression to concatenate the values.
delimiter : Character in between each measure_expr, which is by default a comma (,) .
order_by_clause : Order of the concatenated values.
*/	
	
SUBNO      SUBNAME
---------- ------------------------------
D20        Algorithm
D30        DataStructure
D30        C
D20        C++
D30        Python
D30        DBMS
D10        LinkedList
D20        Matrix
D10        String
D30        Graph
D20        Tree
	
SELECT LISTAGG(SubName, ' , ') WITHIN GROUP (ORDER BY SubName) AS SUBJECTS2 FROM GfG 
	
op
SUBJECTS
-----------------------------------------------------------------------------------
Algorithm , C , C++ , DBMS , DataStructure , Graph , LinkedList , Matrix , Python ,
String , Tree

SELECT SubNo, LISTAGG(SubName, ' , ') WITHIN GROUP (ORDER BY SubName) AS SUBJECTS
FROM   GfG
GROUP BY SubNo;

op
SUBNO      SUBJECTS
------     --------------------------------------------------------------------------------
D10        LinkedList , String
D20         Algorithm , C++ , Matrix , Tree
D30         C , DBMS , DataStructure , Graph , Python

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
