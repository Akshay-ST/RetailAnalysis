
create table Akshay.T1 (
	col1 VARCHAR(1)
);

create table Akshay.T2 (
	col2 VARCHAR(1)
);

INSERT into T1 values
('A'),
('B'),
('B'),
('D'),
('E');

INSERT into T2 values
('A'),
('B'),
('C'),
('D'),
('D');

select t1.col1, t2.col2
from T1 t1 
join T2 t2 on t1.col1 = t2.col2;

/* OP
Col1 Col2
A	A
B	B
B	B
D	D
D	D
*/

select t1.col1, t2.col2
from T1 t1 
left outer join T2 t2 on t1.col1 = t2.col2;

/* op
A	A
B	B
B	B
D	D
D	D
E	NULL
 */


select t1.col1, t2.col2
from T1 t1 
right join T2 t2 on t1.col1 = t2.col2;

/* op
A	A
B	B
B	B
	C
D	D
D	D
*/

select t1.col1, t2.col2
from T1 t1 
left outer join T2 t2 on t1.col1 = t2.col2
UNION 
select t1.col1, t2.col2
from T1 t1 
right join T2 t2 on t1.col1 = t2.col2;

-----------------------------------------------------------------------------------------
create table table_2col_1 (
id int,
value CHAR(1)
);

create table table_2col_2 (
id int,
value CHAR(1)
);

insert into table_2col_1 values
(1,'a'),
(1,'b'),
(1,'c'),
(2,'d'),
(3,'e'),
(3,'f')

insert into table_2col_2 values
(3,'a'),
(3,'b'),
(1,'c'),
(1,'d'),
(4,'f')


select t1.* , t2.*
from table_2col_1 t1
join table_2col_2 t2 on t1.id = t2.id;

/* 
1	a	1	d
1	a	1	c
1	b	1	d
1	b	1	c
1	c	1	d
1	c	1	c
3	e	3	b
3	e	3	a
3	f	3	b
3	f	3	a
 */

select t1.* , t2.*
from table_2col_1 t1
left outer join table_2col_2 t2 on t1.id = t2.id;

/* 
1	a	1	d
1	a	1	c
1	b	1	d
1	b	1	c
1	c	1	d
1	c	1	c
2	d		
3	e	3	b
3	e	3	a
3	f	3	b
3	f	3	a
*/

select t1.* , t2.*
from table_2col_1 t1
right outer join table_2col_2 t2 on t1.id = t2.id;

/* 
1	c	1	c
1	b	1	c
1	a	1	c
1	c	1	d
1	b	1	d
1	a	1	d
3	f	3	a
3	e	3	a
3	f	3	b
3	e	3	b
		4	f
*/

select t1.* , t2.*
from table_2col_1 t1
left outer join table_2col_2 t2 on t1.id = t2.id
UNION 
select t1.* , t2.*
from table_2col_1 t1
right outer join table_2col_2 t2 on t1.id = t2.id
;

/*
1	a	1	d
1	a	1	c
1	b	1	d
1	b	1	c
1	c	1	d
1	c	1	c
2	d		
3	e	3	b
3	e	3	a
3	f	3	b
3	f	3	a
		4	f
 */



