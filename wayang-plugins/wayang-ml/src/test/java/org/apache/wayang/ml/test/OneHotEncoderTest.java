package org.apache.wayang.ml.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;

public class OneHotEncoderTest extends JavaExecutionTestBase {
    @Test
    public void testOneHotEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty("wayang.ml.tuple.average-size", "100");
        final WayangPlan wayangPlan = createWayangPlan("../../README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> executionPlans = buildPlanImplementations(wayangPlan, wayangContext);

        for (final PlanImplementation plan : executionPlans) {
            long[] previous = null;
            for (int i = 0; i < 10; i++) {
                final long[] encoded = OneHotEncoder.encode(plan);
                    if (previous != null) {
                    assertArrayEquals(previous, encoded);
                } else {
                    assertEquals(true, true);
                }
                previous = encoded;
            }
        }
    }
}
