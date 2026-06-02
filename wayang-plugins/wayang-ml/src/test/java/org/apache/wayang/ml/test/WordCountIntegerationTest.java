package org.apache.wayang.ml.test;

import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.costs.DefaultPointwiseCost;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;

public class WordCountIntegerationTest extends JavaExecutionTestBase {
    @Test
    void wordcount() throws Exception {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();

        final String modelPath = WordCountIntegerationTest.class.getResource("/cost_model.onnx").getPath();
        config.setProperty("wayang.ml.model.file", modelPath);

        config.setCostModel(new DefaultPointwiseCost.Factory().makeCost());
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
       
        System.out.println(collector);
    }
}
