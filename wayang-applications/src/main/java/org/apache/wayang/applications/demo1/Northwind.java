package org.apache.wayang.applications.demo1;

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


import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;


import java.util.Arrays;
import java.util.Collection;

public class Northwind {


    public static void main (String[] args) {

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5434/apache_wayang_test_db");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.postgres.jdbc.driverName", "org.postgresql.Driver");


        /* Create a wayang context */
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                ;

        // Get a plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Demo")
                .withUdfJarOf(Northwind.class);



        // Start building a plan
        //For each order, calculate a subtotal for each Order (identified by OrderID)
        Collection<Record> output = planBuilder

                // Extract data from postgres
                .readTable(new PostgresTableSource("local_averages"))

                // Get type, sum, count
                .map(record -> new Record(
                                record.getString(0),
                                record.getString(1),
                                record.getDouble(2),
                                record.getDouble(3)
                        )

                )

                // Compute average as total/sum
                .map(record -> new Record(
                        record.getString(1),
                        record.getDouble(2) / record.getDouble(3),
                        record.getDouble(2),
                        record.getDouble(3)
                        )
                )

                // Sort by order id
                .sort(record -> record.getField(0)
                )
                .collect();

        // helper to print
        printRecords(output);

    }


    private static void printRecords(Collection<Record> output) {
        for(Record record : output) {
            System.out.println(record.getField(0) + " | " + record.getField(1) + " | " + record.getField(2) + " | " + record.getField(3));
        }

    }


}