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
values ('car',8,4),
    ('bike', 10, 5),
    ('bus', 12, 6),
    ('train', 15, 9),
    ('plane', 20, 12);
