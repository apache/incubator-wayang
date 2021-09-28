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
package org.apache.wayang.plugin.hackit.core.identifiers;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * HackitIDGenerator is the base for the generation of ID.
 *
 * {@link org.apache.wayang.plugin.hackit.core.tuple.header.Header} use the ID as and unique identifier of the tuple
 *
 * Type parameters:
 *  <N> – the type of Identifier of the process
 *  <O> - the type of the identifier that is created
 * */
public abstract class HackitIDGenerator<N, O> {

    /**
     * is_address_calculated indicates if the address of the worker is already calculated,
     * because the calculation can be expensive in time.
     */
    private boolean is_address_calculated = false;

    /**
     * address_host is the address where the worker is running.
     */
    protected InetAddress address_host;

    /**
     * This is the identifier of the process, task, or machine; composition depends on the platform
     * this is used for the generators, normally correspond to an {@link Integer}
     * */
    protected N identify_process;

    /**
     * Empty Constructor
     */
    public HackitIDGenerator(){
        this.identify_process = null;
        this.address_host = null;
    }

    /**
     * Constructor with identifier of the process
     * @param identify_process identifier helps on the generation of unique IDs
     */
    public HackitIDGenerator(N identify_process) {
        this.identify_process = identify_process;
        getAddress();
    }

    /**
     * Build the address_host from the host information obtained from the context of execution
     */
    protected void getAddress(){
        if( ! this.is_address_calculated ){
            try {
                this.address_host = InetAddress.getLocalHost();
            } catch (Exception e) {
                this.address_host = null;
            }
        }
    }

    /**
     * Creates the worker id depending on the network context information
     *
     * @return unique number that identifies the worker as unique among the nodes of the cluster
     */
    protected static int createNodeId() {
        int nodeId;
        try {
            StringBuilder sb = new StringBuilder();
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                byte[] mac = networkInterface.getHardwareAddress();
                if (mac != null) {
                    for(int i = 0; i < mac.length; i++) {
                        sb.append(String.format("%02X", mac[i]));
                    }
                }
            }
            nodeId = sb.toString().hashCode();
        } catch (Exception ex) {
            nodeId = (new SecureRandom().nextInt());
        }
        return nodeId;
    }

    /**
     * Generate an unique ID to every {@link org.apache.wayang.plugin.hackit.core.tuple.HackitTuple}, this could have repetition depending on the
     * algorithm of generation
     *
     * @return ID
     */
    public abstract O generateId();
}
