"""
user_id, voted_for (person1,2,3)
"""


output = {}

input = [
    ("user1", "person1"),
    ("user2", "person1"),
    ("user3", "person1"),
    ("user4", "person2"),
    ("user5", "person3")
]


for rec in input:
    if rec[1] in output.keys():
        output[rec[1]] += 1
    else:
        output[rec[1]] = 1
 

print(output)

"""
votes_table : - 
user_id,
candidate_id
candidate_name
"""


"""
with votes_count as (
    select 
        candidate_id,
        candidate_name,
        count(user_id) as total_votes
    from votes_table
    group by 1,2
)

select
    candidate_name,
    DENSE_RANK() OVER ( ORDER BY total_votes DESC) as position
from votes_counts

"""


input_df = spark.readStream....


input_df.groupBy(col("candidate_name")).agg(count(col("user_id")).alias("total_votes"))


"""
start_date, end_date, interval
01/06   01/07   6-hours

start - June 1st, 2025 at 8:00 AM
end - July 1st, 2025 at 8:00 AM
period - every 6 hours
weekend runs - 

2025-06-01 14:00:00 (Sun)
2025-06-01 20:00:00 (Sun)
2025-06-07 02:00:00 (Sat)
2025-06-07 08:00:00 (Sat)
2025-06-07 14:00:00 (Sat)
2025-06-07 20:00:00 (Sat)
2025-06-08 02:00:00 (Sun)
2025-06-08 08:00:00 (Sun)
2025-06-08 14:00:00 (Sun)
2025-06-08 20:00:00 (Sun)
"""


'''
dates = []

# Func: DATENUM: returns num for each date in a year e.g '2026-01-01' = 1
# Func: DATEDIFF: adds an int to a date e.g: DATEDFII("2026-01-01",1,day) = "2026-01-02"


start_date_num = DATENUM(start_date)
end_date_num = DATENUM(end_date)

tenure = end_date_num - start_date_num

for i in range (tenure):
    dates.append(DATEDIFF(start_date, i, day))

dates = [
    '2025-06-01',
    .
    .
    '2025-07-01'
]
'''

# Func: TIMEDIFF: works on timestamp col based on intervals e.g TIMEDIFF("2026-01-01 00:00:00.000", 6, hours) = "2026-01-01 06:00:00.000"


date = start_date

while date <= end_date:
    if WEEKOFDAY(date) == 0 or WEEKOFDAY(date) == 6:
        date = TIMEDIFF(date, 6, hours) 
        print(date)
    else:
        DATEDIFF(date, 5, days)

--------------

calculate number of business day between 21-04-2026 & 28-04-2026 and print them


select 
	date,
	DAYOFWEEK(date) as week_day_num 
	
from dates_tbl
where date between "2026-04-21" and "2026-04-28"
and week_day_num between 1 and 5



------------------------
consider a huge csv file with 5gb data.
 
1. read the data
2. add a column 'sysdate'
3. write to location


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *



input_file_path = "s://raw_bucket/input.csv"

scchema = StructType([
		StructField("id", IntegerType(), False),
		StructField("Name", StringType(), True),
		.
		.
		])
		
spark = SparkSession \
	.builder \
	.appName("Test") \
	.getOrCreate()
	

input_df = spark.read \
		.format("csv") \
		.schema(schema) \
		.load(input_file_path)
		

input_df.write \
	.format("parquet") \
	.partionBy(col("dept_name")) \
	.path("s3://output_bucket/emp_data/")
	





-------------------------
You are given two tables:
 
Table: Orders
 
order_id
customer_id
order_date
order_amount
 
Table: Customers
 
customer_id
customer_name
 
Task
Write a SQL query to:
 
👉 Find the top 3 customers who have spent the most money in the last 6 months

------------------------

with orders_filtered as (
select * from orders
where order_date between DATEDIFF("order_date", -6, months) and CURRENT_DATE()
),
customers_total_orders as (
select 
	customer_id,
	sum(order_amount) as "last_6_months_orders"
from orders_filtered
group by customer_id
),
customers_ranked as (
select 
	customer_id
	DENSE_RANK() OVER (customer_id ORDER BY last_6_months_orders DESC) as rnk
from customers_total_orders
)
Select
	c.customer_name as customer_name
from customers_ranked cr
join customers c on cr.customer_id = c.customer_id
where rnk <= 3

-------------------
we have a table named "test" with one column in it
 
colA
1
2
3
4
5
 
using table test, print the below:
 
colA colB
1 3
2 7
3 13
4 21
5 1

select
	colA,
	(lead(colA)*lead(colA))-colA
from test
