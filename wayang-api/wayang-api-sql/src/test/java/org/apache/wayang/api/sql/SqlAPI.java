/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql;

import com.google.common.io.Resources;
import org.apache.wayang.api.sql.calcite.utils.ModelParser;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;

import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Iterator;

public class SqlAPI {

    public static void exampleCrossPlatform() throws Exception {
        Configuration configuration = new ModelParser(new Configuration()).setProperties();

        configuration.setProperty("wayang.fs.table.url", "C:/tmp/data/data1.csv");

        SqlContext sqlContext = new SqlContext(configuration);

        Collection<Record> result = sqlContext.executeSql(
                "select * \n"
                        + "from fs.data1 as d \n"
                        + "join postgres.involved as i \n"
                        + "on d.id = i.personid"
        );

        printResults(10, result);
    }

    public static void exampleFs() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.fs.table.url", "/data/Projects/databloom/test-data/orders.csv");

        SqlContext sqlContext = new SqlContext(configuration);

        /*Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders where " +
                "o_totalprice > 100");*/

//        Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders");

        Collection<Record> result = sqlContext.executeSql("Select o_orderkey, o_totalprice from fs.orders where " +
                "o_totalprice > 100000");


        printResults(10, result);

    }


    public static void examplePostgres() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/imdb");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "postgres");

        String calciteModel = Resources.toString(
                SqlAPI.class.getResource("/model.json"),
                Charset.defaultCharset());
        configuration.setProperty("wayang.calcite.model", calciteModel);

        SqlContext sqlContext = new SqlContext(configuration);

        Collection<Record> result = sqlContext.executeSql(
                "select id, title, genre \n"
                        + "from postgres.movie m \n"
                        + "join postgres.movie_genre g \n"
                        + "on m.id = g.movieid"
        );

        printResults(10, result);
    }

    public static void exampleWithPostgres() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/dvdrental");
        configuration.setProperty("wayang.postgres.jdbc.user", "user");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");

        String calciteModel = Resources.toString(
                SqlAPI.class.getResource("/model.json"),
                Charset.defaultCharset());
        configuration.setProperty("wayang.calcite.model", calciteModel);

        SqlContext sqlContext = new SqlContext(configuration);


        Collection<Record> result = sqlContext.executeSql(
                "select actor_id, first_name, last_name from postgres.actor"
        );

        printResults(10, result);
    }


    public static void exampleJoinWithPostgres() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/dvdrental");
        configuration.setProperty("wayang.postgres.jdbc.user", "user");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");

        String calciteModel = Resources.toString(
                SqlAPI.class.getResource("/model.json"),
                Charset.defaultCharset());
        configuration.setProperty("wayang.calcite.model", calciteModel);

        SqlContext sqlContext = new SqlContext(configuration);


        Collection<Record> result = sqlContext.executeSql(
                "select actor_id, category_id from postgres.film_actor a inner join postgres.film_category c on a.film_id = c.film_id"
        );

        printResults(10, result);
    }

    public static void exampleAggregateWithPostgres() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/dvdrental");
        configuration.setProperty("wayang.postgres.jdbc.user", "aditya");
        configuration.setProperty("wayang.postgres.jdbc.password", "12345678");

        String calciteModel = Resources.toString(
                SqlAPI.class.getResource("/model.json"),
                Charset.defaultCharset());
        configuration.setProperty("wayang.calcite.model", calciteModel);

        SqlContext sqlContext = new SqlContext(configuration);


        Collection<Record> result = sqlContext.executeSql(
                "SELECT  staff_id, sum(amount*customer_id*2), count(*), avg(amount) FROM postgres.payment group by staff_id"
        );



        printResults(10, result);
    }

    public static void main(String... args) throws Exception {
//        BasicConfigurator.configure();
//        new SqlAPI().examplePostgres();
//        new SqlAPI().exampleFs();
//        new SqlAPI().exampleWithPostgres();
//        new SqlAPI().exampleJoinWithPostgres();
//        new SqlAPI().exampleCrossPlatform();
        long startTime = System.nanoTime();

        new SqlAPI().exampleAggregateWithPostgres();

        long endTime = System.nanoTime();
        long duration = (endTime - startTime);
        System.out.println("Execution time: " + duration + " nanoseconds");
    }


    private static void printResults(int n, Collection<Record> result) {
        // print up to n records
        int count = 0;
        Iterator<Record> iterator = result.iterator();
        while (iterator.hasNext() && count++ < n) {
            Record record = iterator.next();
            System.out.print(" | ");
            for (int i = 0; i < record.size(); i++) {
                Object val = record.getField(i);
                if (val == null) { System.out.print(" " + " | "); }
                else System.out.print(val.toString() + " | ");
            }
            System.out.println("");
        }
    }
}
