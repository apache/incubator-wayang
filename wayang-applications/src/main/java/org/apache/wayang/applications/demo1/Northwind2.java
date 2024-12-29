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
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;

import java.util.Collection;

public class Northwind2 {

    // Define the lambda function for formatting the output
    private static final FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> udf = tuple -> {
        return tuple.getField0() + ": " + tuple.getField1();
    };

    public static void main (String[] args) {

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5434/apache_wayang_test_db");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.postgres.jdbc.driverName", "org.postgresql.Driver");

        String output_topicName = "t1";

        /* Create a wayang context */
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin())
                ;

        // Get a plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Demo")
                .withUdfJarOf(Northwind2.class);

        // Start building a plan
        //For each order, calculate a subtotal for each Order (identified by OrderID)
        //Collection<Record> output =
                planBuilder

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
                .map( record -> new Record(
                        record.getString(1),
                        record.getDouble(2) / record.getDouble(3),
                        record.getDouble(2),
                        record.getDouble(3)
                        )
                ).withName("data 1")

                .writeKafkaTopic(output_topicName, record -> String.format("%s, %s", record.getField(0), record.getField(1)), "job_test_1",
                        LoadProfileEstimators.createFromSpecification("wayang.java.kafkatopicsink.load", configuration));


    }


}