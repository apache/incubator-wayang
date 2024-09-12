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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.DefaultLoadEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;
import org.apache.wayang.core.plan.wayangplan.UnarySink;
import org.apache.wayang.core.types.DataSetType;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.Properties;

/**
 * This {@link UnarySink} writes all incoming data quanta to a single Kafka topic.
 */
public class KafkaTopicSink<T> extends UnarySink<T> implements Serializable {

    protected String topicName;

    protected TransformationDescriptor<T, String> formattingDescriptor;

    private final static Logger logger = LogManager.getLogger(KafkaTopicSink.class);

    transient KafkaProducer<String, String> producer = null;

    public KafkaTopicSink() {
        super();
    }

    /**
     * Creates a new instance with default formatting.
     *
     * @param topicName   Name of Kafka topic that should be written to
     * @param typeClass   {@link Class} of incoming data quanta
     */
    public KafkaTopicSink(String topicName, Class<T> typeClass) {
        this(
                topicName,
                new TransformationDescriptor<>(
                        Objects::toString,
                        typeClass,
                        String.class,
                        new NestableLoadProfileEstimator(
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 10 * in[0]),
                                new DefaultLoadEstimator(1, 1, .99d, (in, out) -> 1000)
                        )
                )
        );
        System.out.println("### 11 ... ");

    }


    /**
     * Creates a new instance.
     *
     * @param topicName        Name of Kafka topic that should be written to
     * @param formattingFunction formats incoming data quanta to a {@link String} representation
     * @param typeClass          {@link Class} of incoming data quanta
     */
    public KafkaTopicSink(String topicName,
                          TransformationDescriptor.SerializableFunction<T, String> formattingFunction,
                          Class<T> typeClass) {
        this(
                topicName,
                new TransformationDescriptor<>(formattingFunction, typeClass, String.class)
        );
        System.out.println("### 12 ... ");

    }

    /**
     * Creates a new instance.
     *
     * @param topicName          Name of Kafka topic that should be written to
     * @param formattingDescriptor formats incoming data quanta to a {@link String} representation
     */
    public KafkaTopicSink(String topicName, TransformationDescriptor<T, String> formattingDescriptor) {
        super(DataSetType.createDefault(formattingDescriptor.getInputType()));
        this.topicName = topicName;
        this.formattingDescriptor = formattingDescriptor;
        System.out.println("### 13 ... ");
    }

    /**
     * Creates a copied instance.
     *
     * @param that should be copied
     */
    public KafkaTopicSink(KafkaTopicSink<T> that) {
        super(that);
        this.topicName = that.topicName;
        this.formattingDescriptor = that.formattingDescriptor;
        System.out.println("### 14 ... ");
    }

    boolean isInitialized = false;

    public void initProducer( KafkaTopicSink<T> kts ) {
        this.producer = kts.getProducer( null );
    }

    public KafkaProducer<String, String> getProducer(){
        if ( this.producer == null ) {
            this.producer = getProducer( null );
        }
        return this.producer;
    }

    public KafkaProducer<String, String> getProducer( Properties props ){

        if ( props == null ) {
            props = getDefaultProperties();
            System.out.println(">>> Create producer from DEFAULT PROPERTIES.");
            props.list( System.out );
        }
        else {
            System.out.println(">>> Create producer from PROPERTIES: " + props);
        }

        // Continue to set up your test using the mockConsumer
        this.producer = new KafkaProducer<String, String>(props);

        if( !isInitialized ) {
            isInitialized = true;
            System.out.println(">>> KafkaTopicSource isInitialized=" + isInitialized);
        }
        return this.producer;
    }

    /**
     * Load properties from a properties file or alternatively use the default properties with some sensitive values
     * from environment variables.
     *
     * @param propertiesFilePath -  File path or null.
     *
     * @return Properties object
     */
    public static Properties loadConfig(String propertiesFilePath) {
        // wayang-kafka-defaults.properties
        Properties props = new Properties();

        logger.info( "> use properties file in path: " + propertiesFilePath + " for Kafka client configuration.");

        if ( propertiesFilePath == null ) {
            return getDefaultProperties();
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

    public static Properties getDefaultProperties() {

        Properties props = new Properties();

        String BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
        String CLUSTER_API_KEY = System.getenv("CLUSTER_API_KEY");
        String CLUSTER_API_SECRET = System.getenv("CLUSTER_API_SECRET");
        String SR_ENDPOINT = System.getenv("SR_ENDPOINT");
        String SR_API_KEY = System.getenv("SR_API_KEY");
        String SR_API_SECRET = System.getenv("SR_API_SECRET");

        /*
        System.out.println( BOOTSTRAP_SERVER );
        System.out.println( CLUSTER_API_KEY );
        System.out.println( CLUSTER_API_SECRET );
        System.out.println( SR_ENDPOINT );
        System.out.println( SR_API_KEY );
        System.out.println( SR_API_SECRET );
        */

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

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "wayang-kafka-java-source-client-2");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // props.list( System.out );

        return props;
    }

}
