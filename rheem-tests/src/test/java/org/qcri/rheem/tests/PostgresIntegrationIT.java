package org.qcri.rheem.tests;

import org.junit.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.postgres.PostgresPlatform;


import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.*;


/**
 * Test the Postgres integration with Rheem.
 */
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

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        RheemPlan rheemPlan = RheemPlans.postgresReadStdout();
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(PostgresPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testScenario2() throws URISyntaxException, IOException {
        RheemPlan rheemPlan = RheemPlans.postgresScenario2();
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(PostgresPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testScenario3() throws URISyntaxException, IOException {
        RheemPlan rheemPlan = RheemPlans.postgresScenario3();
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(PostgresPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());
        //rheemContext.register(SparkPlatform.getInstance());
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void postgresMixedScenario4() throws URISyntaxException, IOException {
        RheemPlan rheemPlan = RheemPlans.postgresMixedScenario4();
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(PostgresPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.execute(rheemPlan);
    }

}