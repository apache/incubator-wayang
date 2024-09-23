\c postgres;

DROP DATABASE IF EXISTS apache_wayang_test_db;

CREATE DATABASE apache_wayang_test_db;

\c apache_wayang_test_db;

CREATE TABLE local_averages(
                     CategoryID SERIAL PRIMARY KEY,
                     CategoryName varchar(50),
                     CategoryCount integer,
                     CategorySum integer
);
insert into local_averages(CategoryName,CategoryCount,CategorySum)
values ('car',800,400),
    ('bike', 1000, 500),
    ('bus', 1200, 600),
    ('train', 1500, 900),
    ('plane', 2000, 1200);
