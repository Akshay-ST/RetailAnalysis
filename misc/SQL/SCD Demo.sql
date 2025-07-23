SELECT 
	product_id,
	product_category_id,
	product_name,
	product_price,
	department_id,
	department_name,
	category_name
FROM products p,departments d, categories c
where p.product_category_id = c.category_id
  and d.department_id = c.category_department_id
 limit 100;
 
select * from customers c limit 10;