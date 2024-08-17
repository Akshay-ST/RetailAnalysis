CREATE TABLE Ratings (
    Name VARCHAR(50),
    Year INT,
    Rating DECIMAL(2, 1)
);

INSERT INTO Ratings (Name, Year, Rating) VALUES
('Jimmy', 2020, 4.2),
('Joseph', 2020, 4.6),
('Jerry', 2020, 3.6),
('Jimmy', 2021, 3.8),
('Joseph', 2021, 3.2),
('Jerry', 2021, 4.9),
('Jimmy', 2022, 4.5),
('Joseph', 2022, 3.8),
('Jerry', 2022, 4.3),
('Jimmy', 2023, 4.0),
('Joseph', 2023, 4.1),
('Jerry', 2023, 3.0);

select * from Ratings;


select Name,year,rating
from (
	select	
		Name,
		Year,
		rating,
		rank() OVER(PARTITION BY YEAR ORDER BY RATING DESC) as rn
	from Akshay.Ratings
) as r
where rn = 1;




