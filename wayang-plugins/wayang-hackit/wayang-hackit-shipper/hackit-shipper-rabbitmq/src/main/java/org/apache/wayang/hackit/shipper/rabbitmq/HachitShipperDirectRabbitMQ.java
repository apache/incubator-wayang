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
package org.apache.wayang.hackit.shipper.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.wayang.hackit.shipper.rabbitmq.receiver.ReceiverMultiChannelRabbitMQ;
import org.apache.wayang.hackit.shipper.rabbitmq.sender.SenderMultiChannelRabbitMQ;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.Shipper;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;
import org.apache.wayang.plugin.hackit.core.tuple.HackitTuple;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

/** Direct is faster but there is not multiple categories of topics, just a word
 *  In other words you publish based on a word, all the consumers who listen that word will fill the message in their queues
 * */
public class HachitShipperDirectRabbitMQ<K, T, ST, HackSender extends Sender<ST>, HackReceiver
        extends Receiver<HackitTuple<K, T>>>
        extends Shipper<HackitTuple<K, T>, ST, HackSender, HackReceiver> {

    private transient ConnectionFactory connectionFactory = null;

    /** Consumer Info */
    private Connection consumeConnection;
    private Channel consumeChannel;
    private String consumeExchangeName;
    private String queueName;


    @Override
    protected HackSender createSenderInstance() {
        return (HackSender) new SenderMultiChannelRabbitMQ(this.connect());
    }

    @Override
    protected HackReceiver createReceiverInstance() {
        return (HackReceiver) new ReceiverMultiChannelRabbitMQ(this.connect());
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public HackitTuple<K, T> next() {
        return null;
    }

    public Connection connect(){
        if(this.connectionFactory == null){
            Properties prop = new Properties();
            InputStream is = null;

            try {
                //is = new FileInputStream("../../resources/rabbitmq-config.properties");
                is = new FileInputStream("C:\\Users\\Admin\\Desktop\\hackit\\hackit-shipper\\hackit-shipper-rabbitmq\\src\\main\\resources\\rabbitmq-config.properties");
                prop.load(is);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }

            this.connectionFactory= new ConnectionFactory();
            this.connectionFactory.setUsername(prop.getProperty("username"));
            this.connectionFactory.setPassword(prop.getProperty("password"));
            this.connectionFactory.setVirtualHost(prop.getProperty("virtualhost"));
            this.connectionFactory.setHost(prop.getProperty("host"));
            this.connectionFactory.setPort(Integer.parseInt(prop.getProperty("port")));
        }

        try {
            return this.connectionFactory.newConnection();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }

        return null;
    }
}