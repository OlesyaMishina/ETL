--1. Создайте таблицу movies с полями movies_type, director, year_of_issue, length_in_minutes, rate.

create table if not exists movies (
movies_type character varying not null,
director character varying not null,
year_of_issue int not null,
length_in_minutes int not null,
rate float not null
);

--2. Сделайте таблицы для горизонтального партицирования по году выпуска (до 1990, 1990 -2000, 2000- 2010, 2010-2020, после 2020).
create table if not exists year_of_issue_1990 (check(year_of_issue<=1990)) inherits (movies);
create table if not exists year_of_issue_1990_2000 (check(year_of_issue>1990 and year_of_issue<=2000)) inherits (movies);
create table if not exists year_of_issue_2000_2010 (check(year_of_issue>2000 and year_of_issue<=2010)) inherits (movies);
create table if not exists year_of_issue_2010_2020 (check(year_of_issue>2010 and year_of_issue<=2020)) inherits (movies);
create table if not exists year_of_issue_2020 (check(year_of_issue>2020)) inherits (movies);

--3. Сделайте таблицы для горизонтального партицирования по длине фильма (до 40 минута, от 40 до 90 минут, от 90 до 130 минут, 
--более 130 минут).

create table if not exists length_40 (check(length_in_minutes<=40)) inherits(movies);
create table if not exists length_40_90 (check(length_in_minutes>40 and length_in_minutes<90)) inherits(movies);
create table if not exists length_90_130 (check(length_in_minutes>90 and length_in_minutes<130)) inherits(movies);
create table if not exists length_130 (check(length_in_minutes>130)) inherits(movies);

-- 4. Сделайте таблицы для горизонтального партицирования по рейтингу фильма (ниже 5, от 5 до 8, от 8до 10).

create table if not exists rate_5 (check(rate<=5)) inherits(movies);
create table if not exists rate_5_8 (check(rate>5 and rate<=8)) inherits(movies);
create table if not exists rate_8_10 (check(rate>8 and rate<=10)) inherits(movies);

-- 5. Создайте правила добавления данных для каждой таблицы.
-- year_of_issue_insert
create rule year_of_issue_insert_1 as on insert to movies 
where (year_of_issue<=1990)
do instead insert into year_of_issue_1990 values (new.*);

create rule year_of_issue_insert_2 as on insert to movies
where (year_of_issue>1990 and year_of_issue<=2000)
do instead insert into year_of_issue_1990_2000 values(new.*);

create rule year_of_issue_insert_3 as on insert to movies
where(year_of_issue>2000 and year_of_issue<=2010)
do instead insert into year_of_issue_2000_2010 values(new.*);

create rule year_of_issue_insert_4 as on insert to movies
where(year_of_issue>2010 and year_of_issue<=2020)
do instead insert into year_of_issue_2010_2020 values(new.*);

create rule year_of_issue_insert_5 as on insert to movies
where(year_of_issue>2020) 
do instead insert into year_of_issue_2020 values(new.*);

-- length_insert
create rule length_insert_1 as on insert to movies
where(length_in_minutes<=40) 
do instead insert into length_40 values(new.*);

create rule length_insert_2 as on insert to movies
where(length_in_minutes>40 and length_in_minutes<90) 
do instead insert into length_40_90 values(new.*);

create rule length_insert_3 as on insert to movies
where(length_in_minutes>90 and length_in_minutes<130) 
do instead insert into length_90_130 values(new.*);

create rule length_insert_4 as on insert to movies
where(length_in_minutes>130) 
do instead insert into length_130 values(new.*);

-- rate_insert
create rule rate_insert_1 as on insert to movies
where(rate<=5) 
do instead insert into rate_5 values(new.*);

create rule rate_insert_2 as on insert to movies
where(rate>5 and rate<=8) 
do instead insert into rate_5_8 values(new.*);

create rule rate_insert_3 as on insert to movies
where(rate>8 and rate<=10) 
do instead insert into rate_8_10 values(new.*);

-- 6. Добавьте фильмы так, чтобы в каждой таблице было не менее 3 фильмов.
insert into movies values('type A', 'director 45', 1985, 55, 4);
insert into movies values('type B', 'director 90', 1999, 93, 9);
insert into movies values('type C', 'director 3', 2010, 35, 6);
insert into movies values('type D', 'director 4', 2015, 122, 3);
insert into movies values('type B', 'director 5', 2023, 84, 5);
insert into movies values('type E', 'director 2',1971, 22, 10);
insert into movies values('type D', 'director 18',2018, 40, 7);
insert into movies values('type F', 'director 7',1989, 98, 9.2);
insert into movies values('type C', 'director 5', 2000, 66, 4.6);
insert into movies values('type C', 'director 1', 1995, 136, 6.6);
insert into movies values('type F', 'director 4', 2024, 146, 5.7);
insert into movies values('type S', 'director 12', 2006, 16, 9.7);
insert into movies values('type Q', 'director 65', 2007, 165, 3.7);


-- 7. Добавьте пару фильмов с рейтингом выше 10.
insert into movies values('type D', 'director 9', 2022, 97, 12);
insert into movies values('type A', 'director 10', 2013, 52, 11);

-- 8. Сделайте выбор из всех таблиц, в том числе из основной.
-- 9. Сделайте выбор только из основной таблицы.

select * from movies;
select * from only movies;