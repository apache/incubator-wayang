package org.apache.wayang.hackit.shipper.rabbitmq.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.PSProtocol;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class SenderMultiChannelRabbitMQ<T> implements Sender<T>, PSProtocol {
    private Connection connection;
    private Channel channel;

    /** Default values */
    private String exchange_name = "default";
    private String topic_name = "default";

    public SenderMultiChannelRabbitMQ(Connection connection) {
        this.connection = connection;
    }

    @Override
    public void init() {
        try {
            this.channel = connection.createChannel();
            channel.exchangeDeclare(exchange_name, "direct");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void send(T value) {
        try {
            this.channel.basicPublish(
                    this.exchange_name,
                    this.topic_name,
                    null,
                    SerializationUtils.serialize((Serializable) value)
            );
        } catch (IOException e) {
            e.printStackTrace();
        }    }

    @Override
    public void close() {
        try {
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
}