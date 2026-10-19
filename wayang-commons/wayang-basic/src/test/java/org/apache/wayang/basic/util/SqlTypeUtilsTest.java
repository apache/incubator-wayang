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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlTypeUtilsTest {

    @Test
    public void testDetectProduct() {
        assertEquals(DatabaseProduct.POSTGRESQL,
                SqlTypeUtils.detectProduct("jdbc:postgresql://localhost:5432/db"));
        assertEquals(DatabaseProduct.MYSQL,
                SqlTypeUtils.detectProduct("jdbc:mysql://localhost:3306/db"));
        assertEquals(DatabaseProduct.ORACLE,
                SqlTypeUtils.detectProduct("jdbc:oracle:thin:@localhost:1521:xe"));
        assertEquals(DatabaseProduct.SQLITE,
                SqlTypeUtils.detectProduct("jdbc:sqlite:test.db"));
        assertEquals(DatabaseProduct.H2,
                SqlTypeUtils.detectProduct("jdbc:h2:mem:test"));
        assertEquals(DatabaseProduct.DERBY,
                SqlTypeUtils.detectProduct("jdbc:derby:memory:test;create=true"));
        assertEquals(DatabaseProduct.MSSQL,
                SqlTypeUtils.detectProduct("jdbc:sqlserver://localhost:1433;databaseName=db"));
        assertEquals(DatabaseProduct.UNKNOWN, SqlTypeUtils.detectProduct("jdbc:unknown:db"));
    }

    @Test
    public void testGetSqlTypeDefault() {
        assertEquals("INT", SqlTypeUtils.getSqlType(Integer.class, DatabaseProduct.UNKNOWN));
        assertEquals("INT", SqlTypeUtils.getSqlType(int.class, DatabaseProduct.UNKNOWN));
        assertEquals("BIGINT", SqlTypeUtils.getSqlType(Long.class, DatabaseProduct.UNKNOWN));
        assertEquals("BIGINT", SqlTypeUtils.getSqlType(long.class, DatabaseProduct.UNKNOWN));
        assertEquals("SMALLINT", SqlTypeUtils.getSqlType(Short.class, DatabaseProduct.UNKNOWN));
        assertEquals("SMALLINT", SqlTypeUtils.getSqlType(short.class, DatabaseProduct.UNKNOWN));
        assertEquals("DOUBLE", SqlTypeUtils.getSqlType(Double.class, DatabaseProduct.UNKNOWN));
        assertEquals("DOUBLE", SqlTypeUtils.getSqlType(double.class, DatabaseProduct.UNKNOWN));
        assertEquals("FLOAT", SqlTypeUtils.getSqlType(Float.class, DatabaseProduct.UNKNOWN));
        assertEquals("FLOAT", SqlTypeUtils.getSqlType(float.class, DatabaseProduct.UNKNOWN));
        assertEquals("NUMERIC(38,18)", SqlTypeUtils.getSqlType(java.math.BigDecimal.class, DatabaseProduct.UNKNOWN));
        assertEquals("BOOLEAN", SqlTypeUtils.getSqlType(Boolean.class, DatabaseProduct.UNKNOWN));
        assertEquals("BOOLEAN", SqlTypeUtils.getSqlType(boolean.class, DatabaseProduct.UNKNOWN));
        assertEquals("VARCHAR(255)", SqlTypeUtils.getSqlType(String.class, DatabaseProduct.UNKNOWN));
        assertEquals("DATE", SqlTypeUtils.getSqlType(java.sql.Date.class, DatabaseProduct.UNKNOWN));
        assertEquals("DATE", SqlTypeUtils.getSqlType(java.time.LocalDate.class, DatabaseProduct.UNKNOWN));
        assertEquals("TIMESTAMP", SqlTypeUtils.getSqlType(java.sql.Timestamp.class, DatabaseProduct.UNKNOWN));
        assertEquals("TIMESTAMP", SqlTypeUtils.getSqlType(java.time.LocalDateTime.class, DatabaseProduct.UNKNOWN));
    }

    @Test
    public void testPostgresqlOverrides() {
        assertEquals("INT", SqlTypeUtils.getSqlType(Integer.class, DatabaseProduct.POSTGRESQL));
        assertEquals("DOUBLE PRECISION", SqlTypeUtils.getSqlType(Double.class, DatabaseProduct.POSTGRESQL));
        assertEquals("DOUBLE PRECISION", SqlTypeUtils.getSqlType(double.class, DatabaseProduct.POSTGRESQL));
        assertEquals("VARCHAR(255)", SqlTypeUtils.getSqlType(String.class, DatabaseProduct.POSTGRESQL));
        // Short and BigDecimal are not overridden for PostgreSQL, they inherit from the default map.
        assertEquals("SMALLINT", SqlTypeUtils.getSqlType(Short.class, DatabaseProduct.POSTGRESQL));
        assertEquals("SMALLINT", SqlTypeUtils.getSqlType(short.class, DatabaseProduct.POSTGRESQL));
        assertEquals("NUMERIC(38,18)", SqlTypeUtils.getSqlType(java.math.BigDecimal.class, DatabaseProduct.POSTGRESQL));
    }

    @Test
    public void testGetSchema() {
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(TestPojo.class,
                DatabaseProduct.POSTGRESQL);
        // id, name, value, active (from getter/is)
        assertEquals(4, schema.size());

        schema.sort((f1, f2) -> f1.getName().compareTo(f2.getName()));

        assertEquals("active", schema.get(0).getName());
        assertEquals("BOOLEAN", schema.get(0).getSqlType());

        assertEquals("id", schema.get(1).getName());
        assertEquals("INT", schema.get(1).getSqlType());

        assertEquals("name", schema.get(2).getName());
        assertEquals("VARCHAR(255)", schema.get(2).getSqlType());

        assertEquals("value", schema.get(3).getName());
        assertEquals("DOUBLE PRECISION", schema.get(3).getSqlType());
    }

    @Test
    public void testGetSchemaRecord() {
        Record record = new Record(1, "test", 1.5);
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(record, DatabaseProduct.POSTGRESQL,
                null);

        assertEquals(3, schema.size());
        assertEquals("c_0", schema.get(0).getName());
        assertEquals("INT", schema.get(0).getSqlType());
        assertEquals(Integer.class, schema.get(0).getJavaClass());

        assertEquals("c_1", schema.get(1).getName());
        assertEquals("VARCHAR(255)", schema.get(1).getSqlType());
        assertEquals(String.class, schema.get(1).getJavaClass());

        assertEquals("c_2", schema.get(2).getName());
        assertEquals("DOUBLE PRECISION", schema.get(2).getSqlType());
        assertEquals(Double.class, schema.get(2).getJavaClass());
    }

    @Test
    public void testGetSchemaRecordWithNames() {
        Record record = new Record(1, "test");
        String[] names = { "id", "description" };
        List<SqlTypeUtils.SchemaField> schema = SqlTypeUtils.getSchema(record, DatabaseProduct.POSTGRESQL,
                names);

        assertEquals(2, schema.size());
        assertEquals("id", schema.get(0).getName());
        assertEquals("description", schema.get(1).getName());
    }

    public static class TestPojo {
        private int id;
        private String name;
        private Double value;
        private boolean active;
        private String hidden;

        public int getId() {
            return id;
        }

        public String getName() {
            return name;
        }

        public Double getValue() {
            return value;
        }

        public boolean isActive() {
            return active;
        }
    }
}
