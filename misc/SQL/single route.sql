Use Akshay;

create table src_dest(
source varchar(50),
destination varchar(50),
distance integer
);

INSERT INTO src_dest(source, destination, distance) VALUES 
('Alaska', 'Albany', 5166),
('Dover', 'Florida', 1393),
('Illinois', 'Indiana', 279),
('New Mexico', 'New York', 2873),
('Albany', 'Alaska', 5166),
('Ohio', 'Oklahoma', 1383),
('Indiana', 'Illinois', 279),
('Oklahoma', 'Ohio', 1383),
('Frankfort', 'Georgia', 695),
('Georgia', 'Frankfort', 695);


SELECT * FROM src_dest;


SELECT
    CONCAT(
        LEAST(source, destination),
        '-',
        GREATEST(source, destination)
    ) AS route,
    MAX(distance) AS distance
FROM src_dest
GROUP BY 1;


with temp as (
    select 
        greatest(source, destination)   as source,
        least(source, destination)      as destination,
        min(distance)                   as dist
    from src_dest
    group by    
        greatest(source, destination),
        least(source, destination)
)
select 
    CONCAT(source, '-' , destination) as route,
    dist
from temp;
