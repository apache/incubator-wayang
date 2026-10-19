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

package org.apache.wayang.basic.util;

import org.apache.wayang.basic.data.Record;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for mapping Java types to SQL types across different dialects.
 */
public class SqlTypeUtils {

    private static final Map<DatabaseProduct, Map<Class<?>, String>> dialectTypeMaps = new HashMap<>();

    static {
        // Default mappings (Standard SQL)
        Map<Class<?>, String> defaultMap = new HashMap<>();
        defaultMap.put(Integer.class, "INT");
        defaultMap.put(int.class, "INT");
        defaultMap.put(Long.class, "BIGINT");
        defaultMap.put(long.class, "BIGINT");
        defaultMap.put(Short.class, "SMALLINT");
        defaultMap.put(short.class, "SMALLINT");
        defaultMap.put(Double.class, "DOUBLE");
        defaultMap.put(double.class, "DOUBLE");
        defaultMap.put(Float.class, "FLOAT");
        defaultMap.put(float.class, "FLOAT");
        // Explicit precision/scale (matches Spark's DecimalType default); a bare
        // NUMERIC defaults to scale 0 on most engines and would truncate decimals.
        defaultMap.put(BigDecimal.class, "NUMERIC(38,18)");
        defaultMap.put(Boolean.class, "BOOLEAN");
        defaultMap.put(boolean.class, "BOOLEAN");
        defaultMap.put(String.class, "VARCHAR(255)");
        defaultMap.put(Date.class, "DATE");
        defaultMap.put(LocalDate.class, "DATE");
        defaultMap.put(Timestamp.class, "TIMESTAMP");
        defaultMap.put(LocalDateTime.class, "TIMESTAMP");

        dialectTypeMaps.put(DatabaseProduct.UNKNOWN, defaultMap);

        // PostgreSQL Overrides
        Map<Class<?>, String> pgMap = new HashMap<>(defaultMap);
        pgMap.put(Double.class, "DOUBLE PRECISION");
        pgMap.put(double.class, "DOUBLE PRECISION");
        dialectTypeMaps.put(DatabaseProduct.POSTGRESQL, pgMap);

        // Add more dialects here as needed (MySQL, Oracle, etc.)
    }

    /**
     * Detects the database product from a JDBC URL.
     *
     * @param url JDBC URL
     * @return detected DatabaseProduct
     */
    public static DatabaseProduct detectProduct(String url) {
        if (url == null)
            return DatabaseProduct.UNKNOWN;
        String lowerUrl = url.toLowerCase();
        if (lowerUrl.contains("postgresql") || lowerUrl.contains("postgres"))
            return DatabaseProduct.POSTGRESQL;
        if (lowerUrl.contains("mysql"))
            return DatabaseProduct.MYSQL;
        if (lowerUrl.contains("oracle"))
            return DatabaseProduct.ORACLE;
        if (lowerUrl.contains("sqlite")) {
            return DatabaseProduct.SQLITE;
        }
        if (lowerUrl.contains("h2"))
            return DatabaseProduct.H2;
        if (lowerUrl.contains("derby"))
            return DatabaseProduct.DERBY;
        if (lowerUrl.contains("mssql") || lowerUrl.contains("sqlserver"))
            return DatabaseProduct.MSSQL;
        return DatabaseProduct.UNKNOWN;
    }

    /**
     * Returns the SQL type for a given Java class and database product.
     *
     * @param cls     Java class
     * @param product database product
     * @return SQL type string
     */
    public static String getSqlType(Class<?> cls, DatabaseProduct product) {
        Map<Class<?>, String> typeMap = dialectTypeMaps.getOrDefault(product,
                dialectTypeMaps.get(DatabaseProduct.UNKNOWN));
        return typeMap.getOrDefault(cls, "VARCHAR(255)");
    }

    /**
     * Extracts schema information from a POJO class or a Record.
     *
     * @param cls     POJO class
     * @param product database product
     * @return a list of schema fields
     */
    public static List<SchemaField> getSchema(Class<?> cls, DatabaseProduct product) {
        List<SchemaField> schema = new ArrayList<>();
        if (cls == Record.class) {
            // For Record.class without an instance, we can't derive names/types easily
            // Users should use the instance-based getSchema or provide columnNames
            return schema;
        }

        for (java.lang.reflect.Method method : cls.getMethods()) {
            if (java.lang.reflect.Modifier.isStatic(method.getModifiers()) ||
                    method.getParameterCount() > 0 ||
                    method.getReturnType() == void.class ||
                    method.getName().equals("getClass")) {
                continue;
            }

            String name = method.getName();
            String propertyName = null;
            if (name.startsWith("get") && name.length() > 3) {
                propertyName = Character.toLowerCase(name.charAt(3)) + name.substring(4);
            } else if (name.startsWith("is") && name.length() > 2
                    && (method.getReturnType() == boolean.class || method.getReturnType() == Boolean.class)) {
                propertyName = Character.toLowerCase(name.charAt(2)) + name.substring(3);
            }

            if (propertyName != null) {
                schema.add(new SchemaField(propertyName, method.getReturnType(),
                        getSqlType(method.getReturnType(), product)));
            }
        }
        schema.sort(java.util.Comparator.comparing(SchemaField::getName));
        return schema;
    }

    /**
     * Extracts schema information from a Record instance by inspecting its fields.
     *
     * @param record    representative record
     * @param product   database product
     * @param userNames optional user-provided column names
     * @return a list of schema fields
     */
    public static List<SchemaField> getSchema(Record record, DatabaseProduct product, String[] userNames) {
        List<SchemaField> schema = new ArrayList<>();
        if (record == null)
            return schema;

        int size = record.size();
        for (int i = 0; i < size; i++) {
            String name = (userNames != null && i < userNames.length) ? userNames[i] : "c_" + i;
            Object val = record.getField(i);
            Class<?> typeClass = val != null ? val.getClass() : String.class;
            String type = getSqlType(typeClass, product);
            schema.add(new SchemaField(name, typeClass, type));
        }
        return schema;
    }

    public static class SchemaField {
        private final String name;
        private final Class<?> javaClass;
        private final String sqlType;

        public SchemaField(String name, Class<?> javaClass, String sqlType) {
            this.name = name;
            this.javaClass = javaClass;
            this.sqlType = sqlType;
        }

        public String getName() {
            return name;
        }

        public Class<?> getJavaClass() {
            return javaClass;
        }

        public String getSqlType() {
            return sqlType;
        }
    }
}
