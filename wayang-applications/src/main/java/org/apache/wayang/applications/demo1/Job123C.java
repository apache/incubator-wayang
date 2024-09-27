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


import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.genericjdbc.GenericJdbc;
import org.apache.wayang.genericjdbc.operators.GenericJdbcTableSource;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaKafkaTopicSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
* Joining 2 tables , person and orders using GenericJdbc Plugin.
 * Tables reside on multiple instances of mysql platform
 * Test to check the working of generic jdbc plugin for multiple instances of same DBMS */



    public class Job123C {

    private static final Logger logger = LoggerFactory.getLogger(Job123C.class);

    // Define the lambda function for formatting the output
    private static final FunctionDescriptor.SerializableFunction<Tuple2<String, Integer>, String> udf = tuple -> {
        return tuple.getField0() + ": " + tuple.getField1();
    };

    private static JavaKafkaTopicSink<Tuple2<Record, Tuple2<Record, Record>>> getTopicSink(String topicName1) {

        Configuration configuration = new Configuration();

        // We assume, that we write back into the same cluster, to avoid "external copies"...
        Properties props = KafkaTopicSource.getDefaultProperties();

        logger.info("> 0 ... ");

        logger.info( "*** [TOPIC-Name] " + topicName1 + " ***");

        logger.info( ">   Write to topic ... ");

        props.list(System.out);

        JavaKafkaTopicSink<Tuple2<Record, Tuple2<Record, Record>>> sink = new JavaKafkaTopicSink<Tuple2<Record, Tuple2<Record, Record>>>(
                topicName1,
                new TransformationDescriptor<>(
                        // Lambda function to transform the complex Tuple2 type into a flat string
                        f -> {
                            Record record1 = f.getField0(); // First Record
                            Record nestedRecord1 = f.getField1().getField0(); // First Record in the nested Tuple
                            Record nestedRecord2 = f.getField1().getField1(); // Second Record in the nested Tuple

                            // Extract fields from the records and concatenate them as needed
                            String result = String.format("Record1: %s, NestedRecord1: %s, NestedRecord2: %s",
                                    record1.toString(),
                                    nestedRecord1.toString(),
                                    nestedRecord2.toString());
                            return result; // Return the formatted string
                        },
                        // Input and output types
                        (Class<Tuple2<Record, Tuple2<Record, Record>>>) (Class<?>) Tuple2.class,
                        String.class
                )
        );

        return sink;

    }


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
        TableSource la3 = new GenericJdbcTableSource("pgsql3","local_averages");

        FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA1 = record -> record.getField(0);
        FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA2 = record -> record.getField(0);
        FunctionDescriptor.SerializableFunction<Record, Object> keyFunctionLA3 = record -> record.getField(0);
        FunctionDescriptor.SerializableFunction<Tuple2<Record, Record>, Object> keyFunctionLA4 = touple -> touple.getField0().getField(0);

        Collection<Tuple2<Record,Record>> collector = new ArrayList<>();
        Collection<Tuple2<Record,Tuple2<Record, Record>>> collector2 = new ArrayList<>();

        JoinOperator<Record, Record, Object> joinOperator = new JoinOperator<>(
                keyFunctionLA1,
                keyFunctionLA2,
                Record.class,
                Record.class,
                Object.class
        );

        @SuppressWarnings("unchecked")
        Class<Tuple2<Record, Record>> tupleClass = (Class<Tuple2<Record, Record>>) (Class<?>) Tuple2.class;

        JoinOperator<Record, Tuple2<Record, Record>, Object> joinOperator2 = new JoinOperator<Record, Tuple2<Record, Record>, Object>(
                keyFunctionLA3,
                keyFunctionLA4,
                Record.class,
                tupleClass,
                Object.class
        );

        // LocalCallbackSink<Tuple2<Record, Tuple2<Record, Record>>> sink2 = LocalCallbackSink.createCollectingSink(collector2, ReflectionUtils.specify(Tuple2.class));

        JavaKafkaTopicSink<Tuple2<Record, Tuple2<Record, Record>>> sink3 = getTopicSink("t1" );

        la1.connectTo(0,joinOperator,0);
        la2.connectTo(0,joinOperator,1);

        la3.connectTo(0,joinOperator2,0);

        joinOperator.connectTo(0,joinOperator2,1);

        joinOperator2.connectTo(0,sink3,0);

        wayangPlan = new WayangPlan(sink3);

        wayangContext.execute("Multiple instances of same DBMS type test (3 table join in 2 steps)", wayangPlan);

        /*
        int count2 = 10;
        for(Tuple2<Record,Tuple2<Record,Record>> r : collector2) {
            System.out.println(r);
            if(--count2 == 0 ) {
                break;
            }
        }
        */

        System.out.println("*** Job123C *** :: [Done]");

    }

}