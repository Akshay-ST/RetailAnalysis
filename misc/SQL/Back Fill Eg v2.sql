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
COALESCE(category, LAG(category) OVER (ORDER BY order_id
										range between current row and unbounded FOLLOWING) 
 ) AS category,
 brand_name
FROM (
 SELECT *,
 ROW_NUMBER() OVER (ORDER BY (select null) ) AS order_id
 FROM brands b) t

ORDER BY order_id;

------------------------------

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
 

