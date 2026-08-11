-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE SCHEMA IF NOT EXISTS wayang_it;

DROP TABLE IF EXISTS wayang_it.operator_result;
DROP TABLE IF EXISTS wayang_it.orders;
DROP TABLE IF EXISTS wayang_it.customers;

CREATE TABLE wayang_it.orders (
    order_id BIGINT,
    customer_id BIGINT,
    region VARCHAR,
    amount DOUBLE
);

INSERT INTO wayang_it.orders VALUES
    (1, 100, 'AMER', 2200.0),
    (2, 101, 'EMEA',  800.5),
    (3, 100, 'AMER',  680.5),
    (4, 102, 'APAC', 1500.0),
    (5, 101, 'EMEA', 1100.0),
    (6, 100, 'AMER',  950.25);

CREATE TABLE wayang_it.customers (
    cust_id BIGINT,
    name VARCHAR,
    tier VARCHAR
);

INSERT INTO wayang_it.customers VALUES
    (100, 'Acme',   'GOLD'),
    (101, 'Globex', 'SILVER'),
    (102, 'Initech','BRONZE');
