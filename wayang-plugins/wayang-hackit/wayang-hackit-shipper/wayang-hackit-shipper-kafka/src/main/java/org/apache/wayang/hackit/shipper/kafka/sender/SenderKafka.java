package org.apache.wayang.hackit.shipper.kafka.sender;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.PSProtocol;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.util.*;
import java.util.stream.Collectors;

public class SenderKafka<K, T> implements Sender<T>, PSProtocol {

    //TODO Get from configuration
    static Map<String, String> KAFKA_MAPPING;
    static {
        KAFKA_MAPPING = new HashMap<>();
        KAFKA_MAPPING.put("127.0.0.1", "127.0.0.1");
    }
    static Integer numPartitions = 1;
    static Short replicationFactor = 1;


    Producer<K, T> producer;
    Properties config;
    List<String> topics;

    public SenderKafka(Properties config){
        this.config = config;
        this.topics = new ArrayList<>();
    }

    public PSProtocol preAddTopic(String... topic) {
        AdminClient ad = AdminClient.create(config);
        List<String> topicsToCreate = new ArrayList<>();
        Collection<NewTopic> l = Arrays.stream(topic)
                .map(t -> {
                    topicsToCreate.add(t);
                    return new NewTopic(t, numPartitions, replicationFactor);
                })
                .collect(Collectors.toList());

        try {
            final CreateTopicsResult result = ad.createTopics(l);
            result.all().get();
            this.topics.addAll(topicsToCreate);
            topicsToCreate.clear();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to create topic:" + topic, e);
        }

        return this;
    }

    @Override
    public PSProtocol addTopic(String... topic) {

        this.topics.addAll(Arrays.stream(topic).collect(Collectors.toList()));
        return this;
    }

    /*Not used by Kafka*/
    @Override
    public PSProtocol addExchange(String exchange) {
        return null;
    }

    @Override
    public void init() {
        this.producer = new KafkaProducer<>(config);
    }

    @Override
    public void send(T value) {

        for (String topic : topics) {
            producer.send(
                    new ProducerRecord<>(topic, null, value));
        }

    }

    public void send(K key, T value) {

        for (String topic : topics) {
            producer.send(
                    new ProducerRecord<>(topic, key, value));
            // System.out.println(value + " sent to " + topic);
        }

    }

    @Override
    public void close() {
        producer.close();
    }
}
