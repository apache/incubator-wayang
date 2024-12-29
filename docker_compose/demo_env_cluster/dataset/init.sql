-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

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
