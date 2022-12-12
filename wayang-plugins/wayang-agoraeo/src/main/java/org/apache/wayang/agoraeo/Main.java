package org.apache.wayang.agoraeo;

import org.apache.wayang.agoraeo.operators.basic.Sen2CorWrapper;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;

public class Main {
    public static void main(String[] args) {

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(WayangAgoraEO.javaPlugin());

        Configuration config = wayangContext.getConfiguration();
        config.load(ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE));

        String sen2cor = config.getStringProperty("org.apache.wayang.agoraeo.sen2cor.location");
        String l2a_images_folder = config.getStringProperty("org.apache.wayang.agoraeo.images.l2a");

        System.out.println("Running AgoraEO!");

        String order = "--from NOW-30DAY --to NOW --order 33UUU";

        WayangPlan w = alternative2WayangPlan(order, sen2cor, l2a_images_folder, "file:///Users/rodrigopardomeza/files/sen2cor-output-agoraeo.txt");

        wayangContext.execute(w, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

    }

    public static WayangPlan alternative2WayangPlan(
            String order,
            String sen2cor,
            String l2a_location,
            String outputFileUrl
    ) {

        SentinelSource source = new SentinelSource(order);

        Sen2CorWrapper toL2A = new Sen2CorWrapper(sen2cor, l2a_location);

        /* TODO: BigEarthNet Pipeline */

        TextFileSink<String> sink = new TextFileSink<>(outputFileUrl, String.class);

        source.connectTo(0,toL2A,0);
        toL2A.connectTo(0,sink,0);


        return new WayangPlan(sink);
    }




//    public static WayangPlan createWayangPlan(
//            String cmd,
//            String outputFileUrl
//    ) {
//
//        IteratorSentinelDownload<File> iter = new FileIteratorSentinelDownload("Sentinel 2 - API", cmd);
//
//        /* Might replay the name of the downloaded file */
//        SentinelSource<File> source = new SentinelSource<>(iter, File.class);
//
//        FlatMapOperator<File, String> files_lines = new FlatMapOperator<>(
//                t -> {
//                    try {
//                        final InputStream inputStream = Files.newInputStream(t.toPath());
//                        Stream<String> rough_lines = new BufferedReader(new InputStreamReader(inputStream)).lines();
//                        return rough_lines::iterator;
//                    } catch (IOException e) {
//                        throw new WayangException(String.format("Reading %s failed.", t), e);
//                    }
//                },
//                File.class,
//                String.class
//        );
//        files_lines.setName("files giving lines");
//
//        FlatMapOperator<String, String> words = new FlatMapOperator<>(
//                line -> Arrays.asList(line.split("\\W+")),
//                String.class,
//                String.class
//        );
//        words.setName("words");
//
//        // for each word transform it to lowercase and output a key-value pair (word, 1)
//        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
//                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
//                        DataUnitType.createBasic(String.class),
//                        DataUnitType.createBasicUnchecked(Tuple2.class)
//                ), DataSetType.createDefault(String.class),
//                DataSetType.createDefaultUnchecked(Tuple2.class)
//        );
//        mapOperator.setName("To lower case, add counter");
//
//
//        // groupby the key (word) and add up the values (frequency)
//        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
//                new TransformationDescriptor<>(pair -> pair.field0,
//                        DataUnitType.createBasicUnchecked(Tuple2.class),
//                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
//                ((a, b) -> {
//                    a.field1 += b.field1;
//                    return a;
//                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
//                DataUnitType.createBasicUnchecked(Tuple2.class)
//        ), DataSetType.createDefaultUnchecked(Tuple2.class)
//        );
//        reduceByOperator.setName("Add counters");
//
//        TextFileSink<Tuple2> sink = new TextFileSink<>(outputFileUrl, Tuple2.class);
//        sink.setName("Collect result");
//
//
//        source.connectTo(0, files_lines, 0);
//        files_lines.connectTo(0, words,0);
//        words.connectTo(0, mapOperator, 0);
//        mapOperator.connectTo(0, reduceByOperator, 0);
//        reduceByOperator.connectTo(0, sink, 0);
//
//        return new WayangPlan(sink);
//    }
}