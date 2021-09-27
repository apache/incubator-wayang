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

package org.apache.wayang.jdbc.execution;

import java.sql.Connection;
import java.sql.DriverManager;
import org.apache.wayang.core.api.exception.WayangException;

/**
 * This class describes a database.
 */
public class DatabaseDescriptor {

    private final String jdbcUrl, user, password, jdbcDriverClassName;

    /**
     * Creates a new instance.
     *
     * @param jdbcUrl             JDBC URL to the database
     * @param user                <i>optional</i> user name or {@code null}
     * @param password            <i>optional</i> password or {@code null}
     * @param jdbcDriverClassName name of the JDBC driver {@link Class} to access the database;
     *                            required for {@link #createJdbcConnection()}
     */
    public DatabaseDescriptor(String jdbcUrl, String user, String password, String jdbcDriverClassName) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.jdbcDriverClassName = jdbcDriverClassName;
    }

    /**
     * Creates a {@link Connection} to the database described by this instance.
     *
     * @return the {@link Connection}
     */
    public Connection createJdbcConnection() {
        try {
            Class.forName(this.jdbcDriverClassName);
        } catch (Exception e) {
            throw new WayangException(String.format("Could not load JDBC driver (%s).", this.jdbcDriverClassName), e);
        }

        try {
            return DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
        } catch (Throwable e) {
            throw new WayangException(String.format(
                    "Could not connect to database (%s) as %s with driver %s.", this.jdbcUrl, this.user, this.jdbcDriverClassName
            ), e);
        }
    }
}
