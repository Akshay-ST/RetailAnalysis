# Write your MySQL query statement below

select a.* from (
select

    case 
        when (w2.temperature > w1.temperature and w2.recordDate > w1.recordDate)
        then w2.id
        when (w2.temperature > w1.temperature and w2.recordDate < w1.recordDate)
        then w1.id
        else null
    end Id

from Weather w1
join Weather w2
on w2.id = 1+w1.id
#where w2.temperature > w1.temperature and w2.recordDate > w1.recordDate
) a
where Ida is not null
;


| id | recordDate | temperature |
| -- | ---------- | ----------- |
| 1  | 2000-12-16 | 3           |
| 2  | 2000-12-15 | -1          |

