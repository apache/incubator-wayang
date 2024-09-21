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
values ('car',80,40),
    ('bike', 100, 50),
    ('bus', 120, 60),
    ('train', 150, 90),
    ('plane', 200, 120);
