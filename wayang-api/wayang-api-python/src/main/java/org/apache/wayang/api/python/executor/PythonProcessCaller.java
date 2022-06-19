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

import java.lang.ProcessBuilder.Redirect;
import org.apache.wayang.api.python.function.PythonCode;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import org.apache.wayang.core.util.ReflectionUtils;

public class PythonProcessCaller {

    private Thread process;
    private Socket socket;
    private ServerSocket serverSocket;
    private boolean ready;

    //TODO How to get the config
    private Configuration configuration;

    public PythonProcessCaller(PythonCode serializedUDF){

        //TODO create documentation to how to the configuration in the code
        this.configuration = new Configuration();
        this.configuration.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));
        this.ready = false;
        byte[] addr = new byte[4];
        addr[0] = 127; addr[1] = 0; addr[2] = 0; addr[3] = 1;

        try {
            /*TODO should NOT be assigned an specific port, set port as 0 (zero)*/
            this.serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(addr));

            Runnable run1 = () -> {
                ProcessBuilder pb = new ProcessBuilder(
                    Arrays.asList(
                        "python3",
                        this.configuration.getStringProperty("wayang.api.python.worker")
                    )
                );
                Map<String, String> workerEnv = pb.environment();
                workerEnv.put("PYTHON_WORKER_FACTORY_PORT",
                    String.valueOf(this.serverSocket.getLocalPort()));

                // TODO See what is happening with ENV Python version
                workerEnv.put(
                    "PYTHONPATH",
                    this.configuration.getStringProperty("wayang.api.python.path")
                );

                pb.redirectOutput(Redirect.INHERIT);
                pb.redirectError(Redirect.INHERIT);
                try {
                    pb.start();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            };
            this.process = new Thread(run1);
            this.process.start();

            // Redirect worker stdout and stderr
            //IDK redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

            // Wait for it to connect to our socket
            this.serverSocket.setSoTimeout(100000);

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

    public Thread getProcess() {
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
            this.process.interrupt();
            this.socket.close();
            this.serverSocket.close();
            System.out.println("Everything closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
