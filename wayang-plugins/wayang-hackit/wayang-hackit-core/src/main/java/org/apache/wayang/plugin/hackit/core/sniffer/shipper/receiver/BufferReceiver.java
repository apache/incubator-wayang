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
 *
 * @param <T>
 */
public class BufferReceiver<T> implements Serializable {
    //TODO implement the doble buffering
    private transient Queue<T> queue;

    //TODO implement the server to receive the messages
    public boolean start(){
        return true;
    }

    //TODO registrer on the rest of the worker
    public boolean register(){
        return true;
    }

    public boolean existQueue(){
        return false;
    }

    public void put(T value){
        if(this.queue == null){
            this.queue = new LinkedBlockingQueue<>();
        }
        this.queue.add(value);
    }
}
