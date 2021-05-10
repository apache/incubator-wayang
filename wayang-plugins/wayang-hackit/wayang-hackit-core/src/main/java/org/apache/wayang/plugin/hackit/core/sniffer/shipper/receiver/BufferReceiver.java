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
package org.apache.wayang.plugin.hackit.core.sniffer.shipper.receiver;

import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * BufferReceiver is component where the element getted by the {@link Receiver} are stored waiting for be consumed
 *
 * @param <T> type of the element that it will receive
 */
//TODO: Implement this class well
public class BufferReceiver<T> implements Serializable {

    //TODO implement the doble buffering
    /**
     * queue is a {@link Queue} where the element are storage waiting to be consumed
     */
    private transient Queue<T> queue;

    /**
     * Start the Buffer to be enable to get new {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     *
     * @return True if the Buffer start without problem, False in other cases
     */
    //TODO implement the server to receive the messages
    public boolean start(){
        return true;
    }


    //TODO registrer on the rest of the worker; validate if is need
    public boolean register(){
        return true;
    }

    //TODO Validate if is need
    public boolean existQueue(){
        return false;
    }

    /**
     * Insert a new value on the {@link BufferReceiver}
     *
     * @param value to be insert on the buffer
     */
    public void put(T value){
        if(this.queue == null){
            this.queue = new LinkedBlockingQueue<>();
        }
        this.queue.add(value);
    }
}
