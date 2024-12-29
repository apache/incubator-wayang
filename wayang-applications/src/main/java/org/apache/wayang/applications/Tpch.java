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
package org.apache.wayang.applications;

import org.apache.spark.internal.config.R;
import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.MapDataQuantaBuilder;
import org.apache.wayang.api.UnarySourceDataQuantaBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;
import org.apache.wayang.spark.Spark;

import java.util.Collection;

/**
 * Example query that joins orders table with lineitem. Orders table is assumed
 * on PostgreSql and lineitem table is assumed on file.
 */
public class Tpch {

    public static void main(String [] args) {
        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/apache_wayang_test_db");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");

        // Create a wayang context
        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        // Get a plan builder
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("Demo")
                .withUdfJarOf(Tpch.class);

        // Read jdbc table from postgresql
        DataQuantaBuilder<?, Record> jdbcSource = planBuilder.readTable(
                new PostgresTableSource("local_averages"));

        // Read file
        DataQuantaBuilder<?, String> fileSource = planBuilder.readTextFile(
                "file:///Users/kamir/GITHUB.active/kamir-incubator-wayang/wayang-applications/data/case-study/catalog.dat");

        // Extract orderkey, quantity from lineitem table
        MapDataQuantaBuilder<?, Record> lineitem = fileSource.map(line -> {
            String []cols = line.split(",");
            return new Record( Integer.parseInt(cols[1]), Double.parseDouble(cols[2]), cols[0]);
        });

        // Get orderkey, totalprice (from jdbc) and join with lineitem
        Collection<Tuple2<Record, Record>> output = jdbcSource
                .map(record -> new Record(
                        record.getInt(0),
                        record.getDouble(3)))
                .join(x->x.getInt(0),lineitem,x->x.getInt(0)).

                collect();

        printRecords(output);
    }

    private static void printRecords(Collection<Tuple2<Record, Record>> output) {
        for(Tuple2<Record,Record> record : output) {
            System.out.println(record.getField0().getField(0)
                    + " | " + record.getField0().getField(1)
                    + " | " + record.getField1().getField(0)
                    + " | " + record.getField1().getField(1));
        }
    }
}