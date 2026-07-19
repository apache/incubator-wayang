/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.jdbc.server;

import org.apache.wayang.jdbc.protocol.MessageEnvelope;
import org.apache.wayang.jdbc.protocol.io.ProtocolMessageCodec;

import java.io.IOException;
import java.net.Socket;

/**
 * Handles one TCP client connection.
 */
class JdbcClientHandler implements Runnable {

    private final Socket socket;

    private final JdbcRequestDispatcher dispatcher;

    private final ProtocolMessageCodec codec;

    JdbcClientHandler(final Socket socket, final JdbcRequestDispatcher dispatcher) {
        this.socket = socket;
        this.dispatcher = dispatcher;
        this.codec = new ProtocolMessageCodec();
    }

    @Override
    public void run() {
        try (Socket clientSocket = this.socket) {
            while (!clientSocket.isClosed()) {
                final MessageEnvelope request = this.codec.read(clientSocket.getInputStream());
                if (request == null) {
                    return;
                }
                final MessageEnvelope response = this.dispatcher.dispatch(request);
                this.codec.write(clientSocket.getOutputStream(), response);
            }
        } catch (IOException ignored) {
            // The driver will surface connection failures through JDBC exceptions.
        }
    }
}
