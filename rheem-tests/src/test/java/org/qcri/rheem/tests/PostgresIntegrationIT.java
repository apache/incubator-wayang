package org.qcri.rheem.tests;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.postgres.PostgresPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Test the Postgres integration with Rheem.
 */
public class PostgresIntegrationIT {

    @Before
    public void setup() {
        PostgresPlatform pg = PostgresPlatform.getInstance();
        PreparedStatement ps = null;

        try {
            Connection connection = pg.getConnection();
            ps = connection.prepareStatement("select * from institute");
            ResultSet rs = ps.executeQuery();
            while ( rs.next() ) {
                System.out.println(rs.getInt(1) + " " + rs.getString(2));
            }
            rs.close();
            ps.close();
            //connection.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() {

    }

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
//        List<String> collector = new LinkedList<>();
//        RheemPlan rheemPlan = RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector);
//
//        // Instantiate Rheem and activate the Java backend.
//        RheemContext rheemContext = new RheemContext();
//        rheemContext.register(JavaPlatform.getInstance());
//
//        // Have Rheem execute the plan.
//        rheemContext.execute(rheemPlan);
//
//        // Verify the plan result.
//        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
//        Assert.assertEquals(lines, collector);
    }

    @Test
    public void testScenario2() throws URISyntaxException, IOException {

    }
}