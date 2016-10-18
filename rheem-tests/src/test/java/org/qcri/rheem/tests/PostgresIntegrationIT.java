package org.qcri.rheem.tests;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.postgres.platform.PostgresPlatform;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * Test the Postgres integration with Rheem.
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
            throw new RheemException(e);
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
            throw new RheemException(e);
        }
    }

}