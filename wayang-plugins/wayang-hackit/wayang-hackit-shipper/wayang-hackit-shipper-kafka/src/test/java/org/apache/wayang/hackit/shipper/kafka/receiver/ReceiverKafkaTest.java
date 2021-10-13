package org.apache.wayang.hackit.shipper.kafka.receiver;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class ReceiverKafkaTest {

    @Test
    void receiveMessage() {

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,  "1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        ReceiverKafka<String, String> receiver = new ReceiverKafka<>(props);
        receiver.init();
        receiver.addTopic("debug");
        Iterator<HackitTuple<String, String>> results = receiver.getElements();
        results.forEachRemaining(t -> System.out.println(t.getValue()));
        receiver.close();
    }
}