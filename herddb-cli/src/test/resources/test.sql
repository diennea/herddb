drop table pippo
go
create table pippo (k1 string primary key)
go
insert into pippo(k1) values('a')
go
insert into pippo(k1) values('b')
go
select * from pippo
go
drop table pippo
