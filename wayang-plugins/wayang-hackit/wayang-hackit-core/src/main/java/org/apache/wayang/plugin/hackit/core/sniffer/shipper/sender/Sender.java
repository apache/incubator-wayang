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
package org.apache.wayang.plugin.hackit.core.sniffer.shipper.sender;

import java.io.Serializable;

/**
 * Sender is the component that send the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} out from
 * the main pipeline
 *
 * @param <T> type of the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} that will be sent out
 */
public interface Sender<T> extends Serializable {

    /**
     * Start the service or connect to the server where the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
     * need to be sent
     */
    void init();

    /**
     * Place in a buffer or send immediately the {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple} this will
     * depend on the configuration
     *
     * @param value to be sent out
     */
    void send(T value);

    /**
     * Terminate the connection and clean the buffers if is needed
     */
    void close();
}
