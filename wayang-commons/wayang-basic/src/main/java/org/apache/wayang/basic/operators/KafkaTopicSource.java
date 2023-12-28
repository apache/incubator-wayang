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

package org.apache.wayang.basic.operators;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.wayang.basic.operators.TextFileSource;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * This source reads messages from a Kafka topic and outputs the messages as data units.
 */
public class KafkaTopicSource extends UnarySource<String> {

    private final static Logger logger = LogManager.getLogger(KafkaTopicSource.class);

    String topicName = null;
    String encoding;

    KafkaConsumer<String, String> consumer = null;

    public KafkaTopicSource(String topicName) {
        this(topicName, "UTF-8");
    }

    public KafkaTopicSource(String topicName, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.topicName = topicName;
        this.encoding = encoding;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public KafkaTopicSource(KafkaTopicSource that) {
        super(that);
        this.topicName = that.getTopicName();
        this.encoding = that.getEncoding();
        this.consumer = that.getConsumer();
    }

    public String getTopicName() {
        return this.topicName;
    }

    public String getEncoding() {
        return this.encoding;
    }

    boolean isInitialized = false;

    public KafkaConsumer<String, String> getConsumer(){
        Properties props = getDefaultProperties();
        this.consumer = new KafkaConsumer<String, String>(props);
        if( !isInitialized ) {
            this.consumer.subscribe(Arrays.asList(topicName));
            isInitialized = true;
        }
        return this.consumer;
    }

    /**
     * Load properties from a properties file or alternatively use the default properties with some sensitive values
     * from environment variables.
     *
     * @param propertiesFilePath -  File path or null.
     *
     * @return Properties object

    public static Properties loadConfig(String propertiesFilePath) {

        Properties props = new Properties();

        logger.info( "> use properties file in path: " + propertiesFilePath + " for Kafka client configuration.");

        if ( propertiesFilePath == null ) {

            return getDefaultProperties(props);
        }
        else {
            try (InputStream input = new FileInputStream(propertiesFilePath)) {
                props.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
            return props;
        }
    }
     */

    public static Properties getDefaultProperties() {

        Properties props = new Properties();

        String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
        String CLUSTER_API_KEY = System.getenv("CLUSTER_API_KEY");
        String CLUSTER_API_SECRET = System.getenv("CLUSTER_API_SECRET");
        String SR_ENDPOINT = System.getenv("SR_ENDPOINT");
        String SR_API_KEY = System.getenv("SR_API_KEY");
        String SR_API_SECRET = System.getenv("SR_API_SECRET");



        System.out.println( BOOTSTRAP_SERVER );
        System.out.println( CLUSTER_API_KEY );
        System.out.println( CLUSTER_API_SECRET );
        System.out.println( SR_ENDPOINT );
        System.out.println( SR_API_KEY );
        System.out.println( SR_API_SECRET );


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




    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new KafkaTopicSource.CardinalityEstimator());
    }



    /*
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        public static final double CORRECTNESS_PROBABILITY = 0.95d;

        /*
         * We expect selectivities to be correct within a factor of {@value #EXPECTED_ESTIMATE_DEVIATION}.
         */
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(KafkaTopicSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );

            // Otherwise calculate the cardinality ...
            // In a streaming app we can't know the nr of messages in a particular topic.
            KafkaTopicSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.");
            return this.FALLBACK_ESTIMATE;
        }

    }

    public void startConsuming() {
        consumer.subscribe(Arrays.asList(topicName));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                processRecord(record);
            }
        }
    }

    private void processRecord(ConsumerRecord<String, String> record) {
        // Implement your record processing logic here
        System.out.printf("key = %s, value = %s%n", record.key(), record.value());
    }


}
