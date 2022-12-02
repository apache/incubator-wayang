package org.apache.wayang.agoraeo;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.wayang.agoraeo.iterators.FileIteratorSentinelDownload;
import org.apache.wayang.agoraeo.iterators.IteratorSentinelDownload;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

public class Main {
    public static void main(String[] args) {

        String input = "file:///Users/rodrigopardomeza/files/file.txt";
        String output = "file:///Users/rodrigopardomeza/files/output-spark.txt";
        System.out.println("Hello AgoraEO!");

        WayangPlan w = basicWayangPlan(input, output);

        WayangContext wayangContext = new WayangContext();
//        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
//        for (String platform : args[0].split(",")) {
//            switch (platform) {
//                case "java":
//                    wayangContext.register(Java.basicPlugin());
//                    break;
//                case "spark":
//                    wayangContext.register(Spark.basicPlugin());
//                    break;
//                default:
//                    System.err.format("Unknown platform: \"%s\"\n", platform);
//                    System.exit(3);
//                    return;
//            }
//        }

        wayangContext.execute(w, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

    }

    public static WayangPlan basicWayangPlan(
            String inputFileUrl,
            String outputFileUrl

    ) {

        TextFileSource textFileSource = new TextFileSource(inputFileUrl);

        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split("\\W+")),
                        String.class,
                        String.class,
                        new ProbabilisticDoubleInterval(100, 10000, 0.8)
                )
        );

        flatMapOperator.setName("Split words");

        FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");

        // write results to a sink
        TextFileSink<String> sink = new TextFileSink<String>(outputFileUrl, String.class);
        sink.setName("Write result");

        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    public static WayangPlan createWayangPlan(
            String cmd,
            String inputFileUrl,
            String outputFileUrl
//            ,
//            Collection<Tuple2<String, Integer>> collector
    ) {

        IteratorSentinelDownload<File> iter = new FileIteratorSentinelDownload("Sentinel 2 - API", cmd);

        /* Might replay the name of the downloaded file */
        SentinelSource<File> source = new SentinelSource<>(iter, File.class);


        TextFileSource textFileSource = new TextFileSource(inputFileUrl);

        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split("\\W+")),
                        String.class,
                        String.class,
                        new ProbabilisticDoubleInterval(100, 10000, 0.8)
                )
        );

        flatMapOperator.setName("Split words");

        FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");

        // write results to a sink
        TextFileSink<String> sink = new TextFileSink<String>(outputFileUrl, String.class);
        sink.setName("Write result");

        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}