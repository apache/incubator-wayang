package org.apache.wayang.spark.test;

import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

import java.util.LinkedList;
import java.util.List;

public class DoubleSpark {
    public static void main(String[] args) {
        List<String> collector = new LinkedList<>();
        TextFileSource textFileSource = new TextFileSource("file:///Users/rodrigopardomeza/files/demo.txt");
        textFileSource.setName("Load file");

        FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");
        filterOperator.addTargetPlatform(Spark.platform("sparky", "wayang-sparky-default.properties"));

        // write results to a sink
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefaultUnchecked(String.class)
        );
        sink.setName("Collect result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, sink, 0);

        WayangContext wayangContext = new WayangContext();
        //wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.multiPlugin("sparky", "wayang-sparky-default.properties"));
//        wayangContext.register(Spark.multiPlugin("other", "wayang-spark-defaults.properties"));
//        wayangContext.register(Spark.basicPlugin());

        System.out.println("here");
        wayangContext.execute(new WayangPlan(sink), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

//        collector.sort((t1 , t2) -> Integer.compare(t2.field1, t1.field1));
        System.out.printf("Found %d words:\n", collector.size());
        collector.forEach(wc -> System.out.printf("%s\n", wc));
    }
}
