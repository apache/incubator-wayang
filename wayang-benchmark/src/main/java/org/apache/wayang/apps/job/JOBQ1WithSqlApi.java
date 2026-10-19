/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.job;

import java.util.Collection;
import java.util.List;

import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.basic.data.Record;

/**
 * Runner for IMDB JO-benchmark query 1. 
 * Host the DB on 5432
 * 
 * ./bin/wayang-submit org.apache.wayang.apps.job.SqlApiExample
 */
public class JOBQ1WithSqlApi {
    public static void main(final String[] args) throws Exception {
        final Configuration configuration = new Configuration();

        final String calciteModel = "{\n" + "    \"version\": \"1.0\",\n" + "    \"defaultSchema\": \"wayang\",\n"
                + "    \"schemas\": [\n" + "        {\n" + "            \"name\": \"postgres\",\n"
                + "            \"type\": \"custom\",\n"
                + "            \"factory\": \"org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory\",\n"
                + "            \"operand\": {\n" + "                \"jdbcDriver\": \"org.postgresql.Driver\",\n"
                + "                \"jdbcUrl\": \"jdbc:postgresql://job:5432/job\",\n"
                + "                \"jdbcUser\": \"postgres\",\n" + "                \"jdbcPassword\": \"postgres\"\n"
                + "            }\n" + "        }\n" + "    ]\n" + "}";

        configuration.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
        configuration.setProperty("wayang.calcite.model", calciteModel);
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        final SqlContext sqlContext = new SqlContext(configuration, List.of(Java.basicPlugin(), Postgres.plugin(), Spark.basicPlugin()));

        final String query = 
                        "SELECT\r\n" + 
                        "    MIN(mc.note) AS production_note,\r\n" + 
                        "    MIN(t.title) AS movie_title,\r\n" + 
                        "    MIN(t.production_year) AS movie_year\r\n" + 
                        "FROM\r\n" + 
                        "    postgres.company_type AS ct\r\n" + 
                        "    INNER JOIN postgres.movie_companies AS mc ON ct.id = mc.company_type_id\r\n" + 
                        "    INNER JOIN postgres.title AS t ON t.id = mc.movie_id\r\n" + 
                        "    INNER JOIN postgres.movie_info_idx AS mi_idx ON t.id = mi_idx.movie_id\r\n" + 
                        "    INNER JOIN postgres.info_type AS it ON it.id = mi_idx.info_type_id\r\n" + 
                        "WHERE ct.kind = 'production companies'\r\n" + 
                        "    AND it.info = 'top 250 rank'\r\n" + 
                        "    AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'\r\n" + 
                        "    AND (mc.note LIKE '%(co-production)%' OR mc.note LIKE '%(presents)%')\r\n" + 
                        "    AND mc.movie_id = mi_idx.movie_id\r\n" + 
                        "";
        
        final Collection<Record> result = sqlContext.executeSql(query);
    }
}
