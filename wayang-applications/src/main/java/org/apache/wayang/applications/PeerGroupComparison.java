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

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.api.MapDataQuantaBuilder;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.genericjdbc.operators.GenericJdbcTableSource;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.postgres.operators.PostgresTableSource;

import java.util.Arrays;
import java.util.Collection;

public class PeerGroupComparison {

    public static void main(String[] args){

        System.out.println( ">>> Apache Wayang Demo Szenario #03");
        System.out.println( "    We use a Postgres DB.");

        if (args != null ) {
            if (args.length > 0) {
                int i = 0;
                for (String arg : args) {
                    String line = String.format("  %d    - %s", i, arg);
                    System.out.println(line);
                    i = i + 1;
                }
            }
        }

        Configuration configuration = new Configuration();
        configuration.setProperty("wayang.pgsql1.jdbc.url", "jdbc:postgresql://localhost:5434/apache_wayang_test_db");
        configuration.setProperty("wayang.pgsql1.jdbc.user", "postgres");
        configuration.setProperty("wayang.pgsql1.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.pgsql1.jdbc.driverName", "org.postgresql.Driver");

        configuration.setProperty("wayang.pgsql2.jdbc.url", "jdbc:postgresql://localhost:5433/apache_wayang_test_db");
        configuration.setProperty("wayang.pgsql2.jdbc.user", "postgres");
        configuration.setProperty("wayang.pgsql2.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.pgsql2.jdbc.driverName", "org.postgresql.Driver");

        configuration.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://localhost:5434/apache_wayang_test_db");
        configuration.setProperty("wayang.postgres.jdbc.user", "postgres");
        configuration.setProperty("wayang.postgres.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.postgres.jdbc.driverName", "org.postgresql.Driver");


        // Get a plan builder.
        WayangContext wayangContext = new WayangContext( configuration )
                .withPlugin(Java.basicPlugin())
                // .withPlugin(Spark.basicPlugin())
                .withPlugin(Postgres.plugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(String.format("WordCount (%s)", "POSTGRES"))
                .withUdfJarOf(PeerGroupComparison.class);

        // Read jdbc table from postgresql
        DataQuantaBuilder<?, Record> jdbcSource0 = planBuilder.readTable(
                new PostgresTableSource("local_averages"));

        // Read file
        DataQuantaBuilder<?, String> fileSource = planBuilder.readTextFile(
                "file:///Users/kamir/GITHUB.active/kamir-incubator-wayang/wayang-applications/data/case-study/catalog.dat");

        // Extract orderkey, quantity from lineitem table
        MapDataQuantaBuilder<?, Record> lineitem = fileSource.map(line -> {
            String []cols = line.split(",");
            return new Record( Integer.parseInt(cols[1]), Double.parseDouble(cols[2]), cols[0]);
        });

        //TableSource ts1 = new GenericJdbcTableSource("pgsql1","local_averages");
        //DataQuantaBuilder<?, Record> jdbcSource1 = planBuilder.readTable( ts1 );

        //TableSource ts2 = new GenericJdbcTableSource("pgsql2","local_averages");
        //DataQuantaBuilder<?, Record> jdbcSource2 = planBuilder.readTable( ts2 );

        //Collection<Tuple2<Record, Record>> output = jdbcSource0
        Collection<Record> output2 = jdbcSource0
                .map(record -> new Record(
                        record.getInt(2),
                        record.getDouble(3)))
                //.join(x->x.getInt(0),lineitem,x->x.getInt(0))
                .collect();
/*
        Collection<Tuple2<Record, Record>> output = jdbcSource1
        //Collection<Record> output = jdbcSource1
                .map(record -> new Record(
                        record.getInt(1),
                        record.getDouble(2)))
                .join(x->x.getInt(0),lineitem,x->x.getInt(0))
                .collect();
*/
        //printRecords(output);

        printRecords1(output2);

        // System.out.println(wordcounts);
        System.out.println( "*** Done. ***" );
    };

    private static void printRecords1(Collection<Record> output) {
        for(Record record : output) {
            System.out.println( record.getField(0)
                    + " | " + record.getField(1)
            );
        }
    }
    private static void printRecords(Collection<Tuple2<Record, Record>> output) {
        for(Tuple2<Record,Record> record : output) {
            System.out.println(record.getField0().getField(0)
                    + " | " + record.getField0().getField(1)
                    + " | " + record.getField1().getField(0)
                    + " | " + record.getField1().getField(1));
        }
    }
};