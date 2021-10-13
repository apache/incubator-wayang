package org.apache.wayang.hackit.shipper.kafka.receiver;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.PSProtocol;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;
import org.apache.wayang.plugin.hackit.core.tuple.header.Header;

import java.util.*;

public class ReceiverKafka<K, T> extends Receiver<HackitTuple<K, T>> implements PSProtocol {

    //TODO Get from configuration
    static Map<String, String> KAFKA_MAPPING;
    static {
        KAFKA_MAPPING = new HashMap<>();
        KAFKA_MAPPING.put("127.0.0.1", "127.0.0.1");
    }
    static Integer numPartitions = 1;
    static Short replicationFactor = 1;


    Consumer<K, T> consumer;
    Properties config;
    List<String> topics;

    public ReceiverKafka(Properties config){
        this.config = config;
        this.topics = new ArrayList<>();
    }


    @Override
    public PSProtocol addTopic(String... topic) {

        this.topics.addAll(Arrays.asList(topic));
        this.consumer.subscribe(this.topics);
        return this;
    }

    @Override
    public PSProtocol addExchange(String exchange) {
        return null;
    }

    @Override
    public void init() {
        this.consumer =
                new KafkaConsumer<>(config);
    }

    @Override
    public Iterator<HackitTuple<K, T>> getElements() {

        final int giveUp = 100;   int noRecordsCount = 0;
        ConsumerRecords<K, T> consumerRecords;
        while (true) {
            consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            List<HackitTuple<K, T>> list = new ArrayList<>();
            consumerRecords.forEach(record ->{
                HackitTuple<K, T> result = new HackitTuple<>(
                        record.value()
                );
                // System.out.println("received " + record.value());
                list.add(result);
            });

            consumer.commitAsync();

            return list.listIterator();
        }
        return null;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
