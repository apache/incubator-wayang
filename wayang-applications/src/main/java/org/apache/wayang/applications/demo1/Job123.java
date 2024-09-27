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
import org.apache.wayang.applications.WordCountOnKafkaTopic;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;

import org.apache.wayang.genericjdbc.operators.*;
import org.apache.wayang.genericjdbc.*;

import java.util.ArrayList;
import java.util.Collection;

/**
* Joining 2 tables , person and orders using GenericJdbc Plugin.
 * Tables reside on multiple instances of mysql platform
 * Test to check the working of generic jdbc plugin for multiple instances of same DBMS */



public class Job123 {

    // Define the lambda function for formatting the output
    private static final FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> udf = tuple -> {
        return tuple.getField0() + ": " + tuple.getField1();
    };


    public static void main(String[] args) {

        String output_topicName = "wayang_demo1";

        WayangPlan wayangPlan;
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

        configuration.setProperty("wayang.pgsql3.jdbc.url", "jdbc:postgresql://localhost:5432/apache_wayang_test_db");
        configuration.setProperty("wayang.pgsql3.jdbc.user", "postgres");
        configuration.setProperty("wayang.pgsql3.jdbc.password", "password");
        // Give the driver name through configuration.
        configuration.setProperty("wayang.pgsql3.jdbc.driverName", "org.postgresql.Driver");

        WayangContext wayangContext = new WayangContext(configuration)
                .withPlugin(Java.basicPlugin())
                .withPlugin(GenericJdbc.plugin())
                ;

        /*Give the name of the jdbc platform along with table name*/
        TableSource la1 = new GenericJdbcTableSource("pgsql1","local_averages");
        TableSource la2 = new GenericJdbcTableSource("pgsql2","local_averages");
        //TableSource la3 = new GenericJdbcTableSource("pgsql3","local_averages");

        FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA1 = record -> record.getField(0);
        FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA2 = record -> record.getField(0);
        //FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA3 = record -> record.getField(0);

        Collection<Tuple2<Record,Record>> collector = new ArrayList<>();

        JoinOperator<Record, Record, Object> joinOperator = new JoinOperator<>(
                keyFunctionLA1,
                keyFunctionLA2,
                Record.class,
                Record.class,
                Object.class
        );

        LocalCallbackSink<Tuple2<Record, Record>> sink = LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));

        la1.connectTo(0,joinOperator,0);
        la2.connectTo(0,joinOperator,1);

        joinOperator.connectTo(0,sink,0);

        /*
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName(String.format("Table-Stats using Java Context on Kafka topic (%s)", output_topicName))
                .withUdfJarOf(WordCountOnKafkaTopic.class)
                ;

        planBuilder.writeKafkaTopic(output_topicName, d -> String.format("%d, %s", d.getField1(), d.getField0()), "job_test_1",
                LoadProfileEstimators.createFromSpecification("wayang.java.kafkatopicsink.load", configuration));
        */

        wayangPlan = new WayangPlan(sink);

        wayangContext.execute("Multiple instances of same DBMS test (2 table join)", wayangPlan);

        int count = 10;
        for(Tuple2<Record,Record> r : collector) {
            System.out.println(r);
            if(--count == 0 ) {
                break;
            }
        }

        System.out.println("*** Job123 *** :: [Done]");

    }

}