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
import java.util.Enumeration;

/**
 * Generate the next ID, N depends of the type that is need
 * */
public abstract class HackitIDGenerator<N, O> {

    private boolean is_address_calculated = false;
    protected InetAddress address_host;

    /** This is the identifier of the process, task, or machine, depends of the platform
     * but is use for the generators.
     * */
    protected N identify_process;

    public HackitIDGenerator(){
        this.identify_process = null;
        this.address_host = null;
    }

    public HackitIDGenerator(N identify_process) {
        this.identify_process = identify_process;
        getAddress();
    }

    protected void getAddress(){
        if( ! this.is_address_calculated ){
            try {
                this.address_host = InetAddress.getLocalHost();
            } catch (Exception e) {
                this.address_host = null;
            }
        }
    }

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

    public abstract O generateId();
}
