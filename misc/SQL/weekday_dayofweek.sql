USE Akshay;
CREATE table dates (
    dt date
);

INSERT into dates (dt) values 
('2026-01-19'), -- mon
('2026-01-20'), -- tue
('2026-01-21'), -- wed
('2026-01-22'), -- thur
('2026-01-23'), -- fri
('2026-01-24'), -- sat
('2026-01-25'), -- sun
('2026-01-26'), -- mon
('2026-01-27'), -- tues
('2026-01-28'), -- wed
('2026-01-29'), -- thur
('2026-01-30'), -- fri
('2026-01-31'); -- Sat

select dt, dayofweek(dt), weekday(dt) from dates;

/*
Aspect	WEEKDAY(date)	DAYOFWEEK(date)
Range	    0-6	1-7 
Monday	    0	2
Tuesday	    1	3
Wednesday	2	4
Thursday	3	5
Friday	    4	6
Saturday	5	7
Sunday	    6	1 
Week Start	Monday (0)	Sunday (1)
*/

select 
    '2026-01-26', 
    DATE_ADD('2026-01-26' , 
    INTERVAL (0 - WEEKDAY('2026-01-26') + 7 ) % 7 day ) as next_monday,
    DATE_ADD('2026-01-26' , 
    INTERVAL (6 - WEEKDAY('2026-01-26') + 7 ) % 7 day ) as next_sunday;


select 
    '2026-01-26', 
    DATE_ADD('2026-01-26' , 
    INTERVAL (1 - DAYOFWEEK('2026-01-26') + 7 ) % 7 day ) as next_sunday,
    DATE_ADD('2026-01-26' , 
    INTERVAL (2 - DAYOFWEEK('2026-01-26') + 7 ) % 7 day ) as next_monday;

select 
    '2026-01-26', 
    DATE_ADD('2026-01-26', interval 7  day);