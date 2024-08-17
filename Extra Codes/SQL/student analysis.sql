SELECT year, student_id, sum(score) as ttl 
FROM "student_db"."students" 
group by 1,2 
order by 3 desc ;

select year, round(avg(score),3) AVG_MATHS_SCORE
from "student_db"."students" 
where subject = 'Math'
group by 1 
order by 2 desc ;


select year, subject, round(avg(score),3) AVG_SCORE
from "student_db"."students" 
group by 1,2
order by 1,3 desc ;

with cte1 as (
    select s.y as year, s.subject, s.AVG_SCORE,
    ROW_NUMBER() OVER(PARTITION BY s.y ORDER BY s.AVG_SCORE desc) rn
    from (
        select year as y, subject, round(avg(score),3) AVG_SCORE
        from "student_db"."students" 
        group by 1,2
    ) as s
)
select year, subject, avg_score
from cte1
where rn=14
order by 1;

