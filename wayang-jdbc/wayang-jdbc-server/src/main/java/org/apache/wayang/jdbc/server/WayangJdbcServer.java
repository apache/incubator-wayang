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

import org.apache.wayang.core.api.Configuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * TCP server for the external Wayang JDBC protocol.
 */
public class WayangJdbcServer implements AutoCloseable {

    private final String host;

    private final int port;

    private final JdbcRequestDispatcher dispatcher;

    private final ExecutorService clientExecutor;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private ServerSocket serverSocket;

    private Thread acceptThread;

    public WayangJdbcServer(
            final String host,
            final int port,
            final SqlQueryExecutor queryExecutor
    ) {
        this.host = host;
        this.port = port;
        this.dispatcher = new JdbcRequestDispatcher(queryExecutor);
        this.clientExecutor = Executors.newCachedThreadPool();
    }

    public void start() throws IOException {
        if (!this.running.compareAndSet(false, true)) {
            throw new IllegalStateException("Wayang JDBC server is already running.");
        }

        this.serverSocket = new ServerSocket(this.port, 50, InetAddress.getByName(this.host));
        this.acceptThread = new Thread(this::acceptLoop, "wayang-jdbc-server-accept");
        this.acceptThread.setDaemon(true);
        this.acceptThread.start();
    }

    public int getPort() {
        if (this.serverSocket == null) {
            return this.port;
        }
        return this.serverSocket.getLocalPort();
    }

    @Override
    public void close() throws IOException {
        this.running.set(false);
        if (this.serverSocket != null) {
            this.serverSocket.close();
        }
        this.clientExecutor.shutdownNow();
        try {
            this.clientExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void main(final String[] args) throws IOException, SQLException, InterruptedException {
        final String host = args.length > 0 ? args[0] : "127.0.0.1";
        final int port = args.length > 1 ? Integer.parseInt(args[1]) : 4567;
        final Configuration configuration = args.length > 2 ? new Configuration(args[2]) : new Configuration();

        final WayangJdbcServer server = new WayangJdbcServer(
                host,
                port,
                new WayangSqlQueryExecutor(configuration)
        );
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                server.close();
            } catch (IOException ignored) {
            }
        }));
        server.start();
        Thread.currentThread().join();
    }

    private void acceptLoop() {
        while (this.running.get()) {
            try {
                final Socket socket = this.serverSocket.accept();
                this.clientExecutor.submit(new JdbcClientHandler(socket, this.dispatcher));
            } catch (IOException e) {
                if (this.running.get()) {
                    this.running.set(false);
                }
            }
        }
    }
}
