create table brands (
category varchar(20),
brand_name varchar(20)
);

select * from brands b ;

insert into brands values
('chocolates','5-star')
,(null,'dairy milk')
,(null,'perk')
,(null,'eclair')
,('Biscuits','britannia')
,(null,'good day')
,(null,'boost');

with cte1 as (
 select *, row_number() over(order by (select null)) as id -- 𝐠𝐫𝐞𝐚𝐭 𝐭𝐫𝐢𝐜𝐤 𝐭𝐨 𝐠𝐞𝐧𝐞𝐫𝐚𝐭𝐞 𝐫𝐨𝐰 𝐧𝐮𝐦𝐛𝐞𝐫
 from brands
),
cte2 as (
 select *, lead(id) over() - 1 as prev_id
 from cte1
 where category is not null
)

select c2.category, c1.brand_name
from cte1 c1
inner join cte2 c2 on c1.id between c2.id and prev_id or (c1.id between c2.id and prev_id is null)
