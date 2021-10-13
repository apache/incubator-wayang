package org.apache.wayang.hackit.shipper.kafka.sender;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class SenderKafkaTest {

    @Test
    void sendMessage() {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //Set acknowledgements for producer requests.
        props.put(ProducerConfig.ACKS_CONFIG,  "1");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        SenderKafka<String, String> sender = new SenderKafka<>(props);
        sender.init();
        sender.addTopic("debug");
        sender.send("perro", "gato");
        sender.send("pulpo", "atun");
        sender.close();
    }

}