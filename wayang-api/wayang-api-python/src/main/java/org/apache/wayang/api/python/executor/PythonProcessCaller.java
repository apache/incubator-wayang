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

package org.apache.wayang.api.python.executor;

import com.google.protobuf.ByteString;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.util.ReflectionUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class PythonProcessCaller {

    private Process process;
    private Socket socket;
    private ServerSocket serverSocket;
    private boolean ready;

    //TODO How to get the config
    private Configuration configuration;

    public PythonProcessCaller(ByteString serializedUDF){

        //TODO create documentation to how to the configuration in the code
        this.configuration = new Configuration();
        this.configuration.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));
        this.ready = false;
        byte[] addr = new byte[4];
        addr[0] = 127; addr[1] = 0; addr[2] = 0; addr[3] = 1;

        try {
            /*TODO should NOT be assigned an specific port, set port as 0 (zero)*/
            this.serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(addr));
            ProcessBuilder pb = new ProcessBuilder(
                    Arrays.asList(
                            this.configuration.getStringProperty("wayang.api.python.path"),
                            this.configuration.getStringProperty("wayang.api.python.worker")
                    )
            );
            Map<String, String> workerEnv = pb.environment();
            workerEnv.put("PYTHON_WORKER_FACTORY_PORT", String.valueOf(this.serverSocket.getLocalPort()));
            workerEnv.put("PYTHONPATH", this.configuration.getStringProperty("wayang.api.python.env.path"));

            pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
            pb.redirectError(ProcessBuilder.Redirect.INHERIT);
            this.process = pb.start();


            // Redirect worker stdout and stderr
            //IDK redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

            // Wait for it to connect to our socket
            this.serverSocket.setSoTimeout(10000);
            try {
                this.socket = this.serverSocket.accept();
                this.serverSocket.setSoTimeout(0);

                if(socket.isConnected())
                    this.ready = true;

            } catch (Exception e) {
                System.out.println(e);
                throw new WayangException("Python worker failed to connect back.", e);
            }
        } catch (Exception e){
            System.out.println(e);
            throw new WayangException("Python worker failed");
        }
    }

    public Process getProcess() {
        return process;
    }

    public Socket getSocket() {
        return socket;
    }

    public boolean isReady(){
        return ready;
    }

    public void close(){
        try {
            this.process.destroy();
            this.socket.close();
            this.serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
