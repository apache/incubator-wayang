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
import java.util.Iterator;

/**
 * Receiver is the component that handle the connection with the side car, and get
 * external elements, this can be instructions to perform or new {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}
 *
 * @param <T> Type of received elements
 */
public abstract class Receiver<T> implements Serializable {

    /**
     * bufferReceiver is an instance of {@link BufferReceiver}
     */
    private transient BufferReceiver<T> bufferReceiver;

    /**
     * Start the the Receiver service that will be waiting the new elements.
     */
    public abstract void init();

    /**
     * Provide the newest elements received, either the process {@link #init()} or the previous call of {@link #getElements()}
     *
     * @return {@link Iterator} with the elements
     */
    public abstract Iterator<T> getElements();

    /**
     * Stop the service and clean the {@link BufferReceiver}
     */
    public abstract void close();
}
