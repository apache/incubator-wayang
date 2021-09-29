package org.apache.wayang.hackit.shipper.kafka.sender;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaHackit<T> implements Sender<T> {

    static Map<String, String> KAFKA_MAPPING;
    transient boolean created = false;
    transient static Producer<String, byte[]> producer;
    transient static ExecutorService pool;
    String topicName;

    static {
        KAFKA_MAPPING = new HashMap<>();
        KAFKA_MAPPING.put("10.4.4.32", "10.4.4.30");
        KAFKA_MAPPING.put("10.4.4.35", "10.4.4.31");
        KAFKA_MAPPING.put("10.4.4.33", "10.4.4.22");
        KAFKA_MAPPING.put("10.4.4.25", "10.4.4.26");
        KAFKA_MAPPING.put("10.4.4.36", "10.4.4.27");
        KAFKA_MAPPING.put("10.4.4.23", "10.4.4.48");
        KAFKA_MAPPING.put("10.4.4.34", "10.4.4.70");
        KAFKA_MAPPING.put("10.4.4.29", "10.4.4.46");
        KAFKA_MAPPING.put("10.4.4.28", "10.4.4.41");
        KAFKA_MAPPING.put("10.4.4.24", "10.4.4.37");
        KAFKA_MAPPING.put("127.0.0.1", "10.4.4.30");
        KAFKA_MAPPING.put("192.168.182.1", "10.4.4.30");
    }

    //private transient
    public void create(){
        //Assign topicName to string variable
        this.topicName = "rheem_debug";
        if(producer != null){
            return;
        }
        String ip;
        String hostname;
        String id_machine;
        try {
            InetAddress info_machine = InetAddress.getLocalHost();
            ip = info_machine.getHostAddress();
            hostname = info_machine.getHostName();
            id_machine = hostname.substring(hostname.length()-2);
        }catch (UnknownHostException e){
            //TODO: modified for the master of the servers
            ip = "127.0.0.1";
            hostname = "localhost";
            id_machine = String.valueOf( (new Random()).nextInt(1000) );
        }

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.format("%s:9092", getBroker(ip)));
        //props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-com.qcri.hackit");
        //Set acknowledgements for producer requests.
        props.put(ProducerConfig.ACKS_CONFIG,  "1");

        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 12);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 1);


        //Specify buffer size in config
        props.put("batch.size", 1);
        //Reduce the no of requests less than 0

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");


        producer = new KafkaProducer<String, byte[]>(props);
        pool = Executors.newFixedThreadPool(5);

    }

    private String getBroker(String ip){
        return KAFKA_MAPPING.get(ip);
    }

    @Override
    public void init() {

    }

    @Override
    public void send(T value) {
        if( ! this.created ){
            this.create();
            this.created = true;
        }
        //System.out.println("sending");
        //final byte[] tmp = SerializationUtils.serialize((Serializable) value);
        pool.execute(
                () -> {
                    producer.send(
                            new ProducerRecord<String, byte[]>(
                                    topicName,
                                    null,
                                    SerializationUtils.serialize((Serializable) value)
                            )
                    );
                }
        );
    }

    @Override
    public void close() {

    }
}
