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

package org.apache.wayang.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.postgres.platform.PostgresPlatform;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Test the Postgres integration with Wayang.
 */
@Ignore("Requires specific PostgreSQL installation.")
public class PostgresIntegrationIT {

    private static final PostgresPlatform pg = PostgresPlatform.getInstance();

    @BeforeClass
    public static void setup() {

        Statement stmt = null;

        try {
            Connection connection = pg.getConnection();
            stmt = connection.createStatement();

            String sql = "DROP TABLE IF EXISTS EMPLOYEE;";
            stmt.executeUpdate(sql);

            sql = "CREATE TABLE EMPLOYEE (ID INTEGER, SALARY DECIMAL);";
            stmt.executeUpdate(sql);

            sql = "INSERT INTO EMPLOYEE (ID, SALARY) VALUES (1, 800.5), (2, 1100),(3, 3000),(4, 5000.8);";
            stmt.executeUpdate(sql);

            stmt.close();

        } catch (SQLException e) {
            throw new WayangException(e);
        }
    }

    @AfterClass
    public static void tearDown() {
        Statement stmt = null;

        try {
            Connection connection = pg.getConnection();
            String sql = "DROP TABLE IF EXISTS EMPLOYEE;";
            stmt = connection.createStatement();
            stmt.executeUpdate(sql);
            stmt.close();

        } catch (SQLException e) {
            throw new WayangException(e);
        }
    }

}
