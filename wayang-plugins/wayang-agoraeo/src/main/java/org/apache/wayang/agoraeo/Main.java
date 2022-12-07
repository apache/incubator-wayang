package org.apache.wayang.agoraeo;

import java.io.*;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.wayang.agoraeo.iterators.FileIteratorSentinelDownload;
import org.apache.wayang.agoraeo.iterators.IteratorSentinelDownload;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

public class Main {
    public static void main(String[] args) {

        String input = "file:///Users/rodrigopardomeza/files/file.txt";
        String output = "file:///Users/rodrigopardomeza/files/output-java.txt";
        System.out.println("Hello AgoraEO!");

        String cmd = "echo \"file:///Users/rodrigopardomeza/files/file.txt\"";

        WayangPlan w = createWayangPlan(cmd, output);

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
//        wayangContext.register(Spark.basicPlugin());
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
            String outputFileUrl
//            ,
//            Collection<Tuple2<String, Integer>> collector
    ) {
        Collection<Tuple2<String, Integer>> collector = new LinkedList<>();


        IteratorSentinelDownload<File> iter = new FileIteratorSentinelDownload("Sentinel 2 - API", cmd);

        /* Might replay the name of the downloaded file */
        SentinelSource<File> source = new SentinelSource<>(iter, File.class);

        FlatMapOperator<File, String> files_lines = new FlatMapOperator<File, String>(
                t -> {
                    try {
                        final InputStream inputStream = Files.newInputStream(t.toPath());
                        Stream<String> rough_lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
                        return rough_lines::iterator;
                    } catch (IOException e) {
                        throw new WayangException(String.format("Reading %s failed.", t), e);
                    }
                },
                File.class,
                String.class
        );

        FlatMapOperator<String, String> words = new FlatMapOperator<String, String>(
                line -> Arrays.asList(line.split("\\W+")),
                String.class,
                String.class
        );

        files_lines.setName("files giving lines");




        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        mapOperator.setName("To lower case, add counter");


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        ), DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        reduceByOperator.setName("Add counters");


        LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        sink.setName("Collect result");


        source.connectTo(0, files_lines, 0);
        files_lines.connectTo(0, words,0);
        words.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}