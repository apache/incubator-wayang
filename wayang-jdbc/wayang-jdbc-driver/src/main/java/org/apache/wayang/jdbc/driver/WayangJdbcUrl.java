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

package org.apache.wayang.jdbc.driver;

import org.apache.wayang.jdbc.protocol.ProtocolConstants;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

public final class WayangJdbcUrl {

    public static final String PREFIX = "jdbc:wayang://";

    private final String host;

    private final int port;

    private final String database;

    private final Properties properties;

    private WayangJdbcUrl(
            final String host,
            final int port,
            final String database,
            final Properties properties
    ) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.properties = properties;
    }

    public static boolean accepts(final String url) {
        return url != null && url.startsWith(PREFIX);
    }

    public static WayangJdbcUrl parse(final String url, final Properties inputProperties) throws SQLException {
        if (!accepts(url)) {
            throw new SQLException("Invalid Wayang JDBC URL: " + url);
        }

        final String uriValue = "wayang://" + url.substring(PREFIX.length());

        final URI uri;
        try {
            uri = new URI(uriValue);
        } catch (URISyntaxException e) {
            throw new SQLException("Could not parse Wayang JDBC URL: " + url, e);
        }

        final String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new SQLException("Wayang JDBC URL must contain a host.");
        }

        final int port = uri.getPort() == -1 ? ProtocolConstants.DEFAULT_PORT : uri.getPort();
        final String database = parseDatabase(uri.getPath());

        final Properties properties = new Properties();
        if (inputProperties != null) {
            properties.putAll(inputProperties);
        }
        parseQueryProperties(uri.getRawQuery()).forEach(properties::setProperty);

        return new WayangJdbcUrl(host, port, database, properties);
    }

    private static String parseDatabase(final String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return null;
        }
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static Map<String, String> parseQueryProperties(final String query) throws SQLException {
        final Map<String, String> properties = new LinkedHashMap<>();
        if (query == null || query.isBlank()) {
            return properties;
        }

        final String[] pairs = query.split("&");
        for (String pair : pairs) {
            final int separator = pair.indexOf('=');
            if (separator <= 0) {
                throw new SQLException("Invalid Wayang JDBC URL query property: " + pair);
            }
            properties.put(pair.substring(0, separator), pair.substring(separator + 1));
        }

        return properties;
    }

    public String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public String getDatabase() {
        return this.database;
    }

    public Properties getProperties() {
        final Properties copy = new Properties();
        copy.putAll(this.properties);
        return copy;
    }
}
