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
package org.apache.wayang.java.test;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;


/**
 * What is this test testin?
 *
 * Currently, it tests only if a KafkaConsumer can connect to a given
 * Kafka cluster using the defaultConnectionProperties.
 *
 * This is helpful during debugging, but it doesn't add any value to the
 * test suite of Apache Wayang. Hence, the test functions are deactivated
 * in the main branch.
 *
 */
class KafkaClientTest {

    @Test
    @Disabled
    void testReadFromKafkaTopic() {

        final String topicName1 = "banking-tx-small-csv";

        System.out.println("> 0 ... ");

        System.out.println( "*** [TOPIC-Name] " + topicName1 + " ***");

        System.out.println( ">   Read from topic ... ");

        System.out.println("> 1 ... ");

        Properties props = getDefaultProperties();

        System.out.println("> 2 ... ");

        props.list(System.out);

        System.out.println("> 3 ... ");

        KafkaConsumer consumer = new KafkaConsumer<String, String>(props);

        try {

            //consumer.subscribe( Arrays.asList(topicName1) );

            System.out.println("> 4 ... ");

            int i=0;
            while (i < 4) {
                ConsumerRecords<String, String> records = consumer.poll(0);
                //ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    processRecord(record);
                    i++;
                }
            }

            System.out.println("> 5 ... " + i);

        }
        catch (Exception ex) {

            ex.printStackTrace();

        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        // Implement your record processing logic here
        System.out.printf("key = %s, value = %s%n", record.key(), record.value());
    }

    public static void main(String[] args){
        KafkaClientTest c = new KafkaClientTest();
        c.testReadFromKafkaTopic();
    }

    public static Properties getDefaultProperties() {

        Properties props = new Properties();

        String BOOTSTRAP_SERVER = null;
        String CLUSTER_API_KEY = null;
        String CLUSTER_API_SECRET = null;
        String SR_ENDPOINT = null;
        String SR_API_KEY = null;
        String SR_API_SECRET = null;

        try {
            BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
            CLUSTER_API_KEY = System.getenv("CLUSTER_API_KEY");
            CLUSTER_API_SECRET = System.getenv("CLUSTER_API_SECRET");
            SR_ENDPOINT = System.getenv("SR_ENDPOINT");
            SR_API_KEY = System.getenv("SR_API_KEY");
            SR_API_SECRET = System.getenv("SR_API_SECRET");
        }
        catch (Exception ex) {
            BOOTSTRAP_SERVER = null;
            CLUSTER_API_KEY = null;
            CLUSTER_API_SECRET = null;
            SR_ENDPOINT = null;
            SR_API_KEY = null;
            SR_API_SECRET = null;
        }

        System.out.println( "BOOTSTRAP_SERVER   : " + BOOTSTRAP_SERVER );
        System.out.println( "CLUSTER_API_KEY    : " + CLUSTER_API_KEY );
        System.out.println( "CLUSTER_API_SECRET : " + CLUSTER_API_SECRET );
        System.out.println( "SR_ENDPOINT        : " + SR_ENDPOINT );
        System.out.println( "SR_API_KEY         : " + SR_API_KEY );
        System.out.println( "SR_API_SECRET      : " + SR_API_SECRET );

        // Set additional properties if needed
        props.put("bootstrap.servers", BOOTSTRAP_SERVER );
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='" + CLUSTER_API_KEY + "' password='" + CLUSTER_API_SECRET + "';");
        props.put("sasl.mechanism", "PLAIN");
        props.put("client.dns.lookup", "use_all_dns_ips");
        props.put("session.timeout.ms", "45000");
        props.put("acks", "all");
        props.put("schema.registry.url", SR_ENDPOINT);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", SR_API_KEY + ":" + SR_API_SECRET );

        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wayang-kafka-java-source-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.list( System.out );

        return props;
    }
}
