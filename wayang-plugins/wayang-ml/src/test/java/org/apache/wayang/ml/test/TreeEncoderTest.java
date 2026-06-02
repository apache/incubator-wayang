package org.apache.wayang.ml.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TreeEncoderTest extends JavaExecutionTestBase {
    @Test
    public void testTreeEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);

        Assertions.assertNotNull(exPlan);
        Assertions.assertNotNull(encoded);
    }
}
