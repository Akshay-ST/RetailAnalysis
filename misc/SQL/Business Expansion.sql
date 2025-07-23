CREATE TABLE business_expansion (
    BUSINESS_DATE DATE,
    CITY_ID INT
);

INSERT INTO business_expansion (BUSINESS_DATE, CITY_ID) VALUES
('2020-01-02', 3),
('2020-07-01', 7),
('2021-01-01', 3),
('2021-02-03', 19),
('2022-12-01', 3),
('2022-12-15', 3),
('2022-02-28', 12);

with CTE as (
    select city_id,
           min(year(business_date)) as min_year
    from business_expansion
    GROUP BY 1
)
select min_year as First_operation_year,
        count(*) as no_of_new_cities
from CTE
GROUP BY 1
order by 1;