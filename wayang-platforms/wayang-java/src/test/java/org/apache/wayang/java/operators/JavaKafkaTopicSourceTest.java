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
package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.KafkaTopicSource;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.execution.JavaExecutor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Test suite for {@link JavaKafkaTopicSource}.
 */
public class JavaKafkaTopicSourceTest extends JavaExecutionOperatorTestBase {

    private Locale defaultLocale;

    /**
     * In locales, where the decimal separator is not "." this rest would fail.
     * Therefore we ensure it's run in a pre-defined locale and we make sure it's
     * reset after the test.
     */
    @Before
    public void setupTest() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
        System.out.println(">>> Test SETUP()");
    }

    @After
    public void teardownTest() {
        System.out.println(">>> Test TEARDOWN()");
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void testA() throws Exception {
        Assert.assertEquals(3, 3);
        System.out.println(">>> Test A");
    }

    @Test
    public void testReadFromKafkaTopic() {

        System.out.println(">>> Test testReadFromKafkaTopic()");

        final String topicName1 = "banking-tx-small-csv";

        System.out.println("> 0 ... ");

        System.out.println( "*** [TOPIC-Name] " + topicName1 + " ***");

        System.out.println( ">   Read from topic ... ");

        System.out.println("> 1 ... ");

        Properties props = KafkaTopicSource.getDefaultProperties();

        System.out.println("> 2 ... ");

        props.list(System.out);

        System.out.println("> 3 ... ");

        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            JavaKafkaTopicSource jks = new JavaKafkaTopicSource( topicName1 );

            System.out.println("> 4 ... ");

            // Execute.
            JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
            JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
            evaluate(jks, inputs, outputs);

            System.out.println("> 5 ... ");

            // Verify the outcome.
            final List<String> result = outputs[0].<String>provideStream().collect(Collectors.toList());

            Assert.assertNotNull(jks);
            Assert.assertNotNull(result);

            System.out.println("> 6 ... ");


        } finally {
            if (javaExecutor != null) javaExecutor.dispose();
        }

    }



    private void processRecord(ConsumerRecord<String, String> record) {
        // Implement your record processing logic here
        System.out.printf("===> processRecord :: key = %s, value = %s%n", record.key(), record.value());
    }




}
