create table Akshay.one_col_score ( score int);

insert into one_col_score (score) values (100),(100),(200),(300),(300);

select * from one_col_score ;

select score,
		ROW_NUMBER() OVER (PARTITION BY score) as row_num,
		RANK() OVER (PARTITION BY score) as rnk,
		DENSE_RANK() OVER (PARTITION BY score) as d_rnk
from one_col_score ;

create table Akshay.two_col_score ( name VARCHAR (1), score int);

insert into two_col_score (Name, score) values 
('A',100),
('B',100),
('C',200),
('D',300),
('E',300);

select * from two_col_score ;

select name, score,
		ROW_NUMBER() OVER (PARTITION BY score) as row_num,
		RANK() OVER (PARTITION BY NAme order by score desc) as rnk,
		DENSE_RANK() OVER (PARTITION BY NAME order by score desc) as d_rnk
from two_col_score ;
