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
package org.apache.wayang.plugin.hackit.core.sniffer.shipper;

import org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver.Receiver;
import org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender.Sender;

import java.io.Serializable;
import java.util.Iterator;

/**
 *
 * @param <T>
 * @param <ST>
 * @param <SenderObj>
 * @param <ReceiverObj>
 */
public abstract class Shipper<T, ST, SenderObj extends Sender<ST>, ReceiverObj extends Receiver<T>> implements Iterator<T>, Serializable {

    /**
     *
     */
    protected Sender sender_instance;

    /**
     *
     */
    protected Receiver receiver_instance;

    /**
     *
     * @return
     */
    protected abstract Sender createSenderInstance();

    /**
     *
     * @return
     */
    protected abstract Receiver createReceiverInstance();

    /**
     * Connect with the a Message queue service
     * @param value
     */
    public void publish(ST value){
        if(this.sender_instance == null){
            throw new RuntimeException("The Sender of the Shipper is not instanciated");
        }
        this.sender_instance.send(value);
    }

    /**
     * To subscribe as a producer
     */
    public void subscribeAsProducer(){
        this.sender_instance = this.createSenderInstance();
        this.sender_instance.init();
    }

    /**
     *
     * @param topic
     */
    public void subscribeAsProducer(String... topic){
        this.subscribeAsProducer("default", topic);
    }

    /**
     *
     * @param metatopic
     * @param topic
     */
    public void subscribeAsProducer(String metatopic, String... topic){
        this.subscribeAsProducer();
        ((PSProtocol)this.sender_instance)
                .addExchange(metatopic)
                .addTopic(topic)
        ;
    }

    /**
     * Close connection
     */
    public void unsubscribeAsProducer(){
        if( this.sender_instance == null) return;
        this.sender_instance.close();
    }

    /**
     * To subscribe/unsubscribe as a consumer
     * metatopic correspond to EXCHANGE_NAME
     * topics correspond to bindingKeys
     */
    public void subscribeAsConsumer(){
        this.receiver_instance = this.createReceiverInstance();
        this.receiver_instance.init();
    }

    /**
     *
     * @param topic
     */
    public void subscribeAsConsumer(String... topic){
        this.subscribeAsProducer("default", topic);
    }

    /**
     *
     * @param metatopic
     * @param topic
     */
    public void subscribeAsConsumer(String metatopic, String... topic){
        this.subscribeAsConsumer();
        ((PSProtocol)this.receiver_instance)
                .addExchange(metatopic)
                .addTopic(topic)
        ;
    }

    /**
     * Close connection
     */
    public void unsubscribeAsConsumer() {
        if( this.receiver_instance == null) return;
        this.receiver_instance.close();
    }

    /**
     *
     */
    public void close(){
        this.unsubscribeAsConsumer();
        this.unsubscribeAsProducer();
    }

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract T next();

    /**
     *
     * @return
     */
    public Iterator<T> getNexts(){
        if( this.receiver_instance == null){
            throw new RuntimeException("The Receiver of the Shipper is not instanciated");
        }
        return this.receiver_instance.getElements();
    }
}
