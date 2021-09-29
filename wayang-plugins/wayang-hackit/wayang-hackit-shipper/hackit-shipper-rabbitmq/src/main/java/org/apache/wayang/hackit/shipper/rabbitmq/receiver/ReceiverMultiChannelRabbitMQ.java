package org.apache.wayang.hackit.shipper.rabbitmq.receiver;

import com.rabbitmq.client.*;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.PSProtocol;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ReceiverMultiChannelRabbitMQ<K, T> extends Receiver<HackitTuple<K, T>> implements PSProtocol {

    private transient Connection connection;
    private transient Channel channel;
    private transient Thread thread_collecting;
    private transient ArrayList<HackitTuple<K, T>> collection;

    /** Default values */
    private String exchange_name = "default_consumer";
    private String topic_name = "default_consumer";
    private String queue_name= "";


    public ReceiverMultiChannelRabbitMQ(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void init() {
        try {
            this.channel =  this.connection.createChannel();
            this.queue_name = this.channel.queueDeclare().getQueue();
            System.out.println(this.queue_name);
            this.collection = new ArrayList<>();

            final ReceiverMultiChannelRabbitMQ<K, T> thos = this;
            this.thread_collecting = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        channel.basicConsume(queue_name, true, new DefaultConsumer(channel) {
                            @Override
                            public void handleDelivery(String consumerTag,
                                                       Envelope envelope,
                                                       AMQP.BasicProperties properties,
                                                       byte[] body)
                                    throws IOException
                            {
                                HackitTuple<K, T> elem = SerializationUtils.deserialize(body);
                                thos.addElement(elem);
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            });
            this.thread_collecting.run();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<HackitTuple<K, T>> getElements() {
        return this._getElements();
    }

    @Override
    public void close() {
        try {
            this.thread_collecting.stop();
            this.channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public PSProtocol addTopic(String... topic) {
        if(topic.length > 1) {
            this.topic_name = Arrays.stream(topic).collect(Collectors.joining("."));
        }else {
            this.topic_name = topic[0];
        }
        return this;
    }

    @Override
    public PSProtocol addExchange(String exchange) {
        this.exchange_name = exchange;
        return this;
    }

    public synchronized void addElement(HackitTuple<K, T> element){
        this.collection.add(element);
    }

    private synchronized Iterator<HackitTuple<K, T>> _getElements(){
        if(this.collection.size() == 0){
            return Collections.emptyIterator();
        }
        Iterator<HackitTuple<K, T>> tmp = this.collection.iterator();
        this.collection = new ArrayList<>();
        return tmp;
    }
}