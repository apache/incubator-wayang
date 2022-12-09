package org.apache.wayang.agoraeo;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Stream;

import org.apache.wayang.agoraeo.iterators.FileIteratorSentinelDownload;
import org.apache.wayang.agoraeo.iterators.IteratorSentinelDownload;
import org.apache.wayang.agoraeo.iterators.StringIteratorSentinelDownload;
import org.apache.wayang.agoraeo.operators.basic.SentinelSource;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
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
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;

public class Main {
    public static void main(String[] args) {

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(WayangAgoraEO.javaPlugin());

        Configuration config = wayangContext.getConfiguration();//.fork(this.name);

        config.load(ReflectionUtils.loadResource(WayangAgoraEO.DEFAULT_CONFIG_FILE));

        String path_python = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.python.location").isPresent()) {
            path_python = config.getOptionalStringProperty("org.apache.wayang.agoraeo.python.location").get();
        } else {
            throw new WayangException("No Python Interpreter to run Minimal Download");
        }

        String downloader_location = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.minimaldownload.location").isPresent()) {
            downloader_location = config.getOptionalStringProperty("org.apache.wayang.agoraeo.minimaldownload.location").get();
        } else {
            throw new WayangException("No defined Minimal Download path");
        }

        String sen2cor = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.sen2cor.location").isPresent()) {
            sen2cor = config.getOptionalStringProperty("org.apache.wayang.agoraeo.sen2cor.location").get();
        } else {
            throw new WayangException("No defined Minimal Download path");
        }

        String l2a_images_folder = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.images.l2a").isPresent()) {
            l2a_images_folder = config.getOptionalStringProperty("org.apache.wayang.agoraeo.images.l2a").get();
        } else {
            throw new WayangException("No Python Interpreter to run Minimal Download");
        }

        String user = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.user").isPresent()) {
            user = config.getOptionalStringProperty("org.apache.wayang.agoraeo.user").get();
        } else {
            throw new WayangException("No Python Interpreter to run Minimal Download");
        }

        String pass = "";
        if (config.getOptionalStringProperty("org.apache.wayang.agoraeo.pass").isPresent()) {
            pass = config.getOptionalStringProperty("org.apache.wayang.agoraeo.pass").get();
        } else {
            throw new WayangException("No Python Interpreter to run Minimal Download");
        }

        System.out.println("Hello AgoraEO!");

        String order = " --url https://scihub.copernicus.eu/dhus --from NOW-30DAY --to NOW --order 32VNM";
        String cmd =
                path_python + " " + downloader_location + " --user " + user + " --password " + pass + order;


        System.out.println(path_python);
        System.out.println(downloader_location);
        System.out.println(l2a_images_folder);
        System.out.println(cmd);
        WayangPlan w = alternative2WayangPlan(cmd, sen2cor, l2a_images_folder, "");

        wayangContext.execute(w, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

    }

    public static WayangPlan alternativeWayangPlan(
            String cmd,
            String outputFileUrl
    ) {
        IteratorSentinelDownload<File> iter = new FileIteratorSentinelDownload("Sentinel 2 - API", cmd);

        SentinelSource<File> source = new SentinelSource<>(iter, File.class);

        /*TODO Replace String to call for Sen2Cor and retrieve output of the command*/
        MapOperator<File, String> files_names = new MapOperator<>(
                t -> t.getAbsolutePath(),
                File.class,
                String.class
        );

//        FlatMapOperator<File, String> files_names = new FlatMapOperator<>(
//                t -> {
//                    try {
//                        String command = "~/Downloads/Sen2Cor-02.10.01-Darwin64/bin/L2A_Process\n" +
//                                "~/tu-berlin/agoraeo/images/33UVT/S2B_MSIL1C_20221119T101229_N0400_R022_T33UVT_20221119T104925.SAFE\n" +
//                                "--output_dir ~/tu-berlin/agoraeo/images/33UVT/L2A/";
//                        Process process = Runtime.getRuntime().exec(command);
//                        Iterator<String> input = new BufferedReader(
//                                new InputStreamReader(
//                                        process.getInputStream()
//                                )
//                        ).lines().iterator();
//                        return () -> input;
//                    } catch (IOException e) {
//                        throw new RuntimeException(e);
//                    }
//                },
//                File.class,
//                String.class
//        );


        TextFileSink<String> sink = new TextFileSink<>(outputFileUrl, String.class);

        source.connectTo(0,files_names,0);
        files_names.connectTo(0,sink,0);


        return new WayangPlan(sink);
    }

    public static WayangPlan alternative2WayangPlan(
            String cmd,
            String sen2cor,
            String l2a_location,
            String outputFileUrl
    ) {
        IteratorSentinelDownload<String> iter = new StringIteratorSentinelDownload("Sentinel 2 - API", cmd);

        SentinelSource<String> source = new SentinelSource<>(iter, String.class);

        FlatMapOperator<String, String> files_names = new FlatMapOperator<>(
                t -> {
                    try {
                        String command = sen2cor + " " +
                                t + " " +
                                " --output_dir " + l2a_location;
                        Process process = Runtime.getRuntime().exec(command);
                        Iterator<String> input = new BufferedReader(
                                new InputStreamReader(
                                        process.getInputStream()
                                )
                        ).lines().iterator();
                        return () -> input;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                },
                String.class,
                String.class
        );


        TextFileSink<String> sink = new TextFileSink<>(outputFileUrl, String.class);

        source.connectTo(0,files_names,0);
        files_names.connectTo(0,sink,0);


        return new WayangPlan(sink);
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
    ) {

        IteratorSentinelDownload<File> iter = new FileIteratorSentinelDownload("Sentinel 2 - API", cmd);

        /* Might replay the name of the downloaded file */
        SentinelSource<File> source = new SentinelSource<>(iter, File.class);

        FlatMapOperator<File, String> files_lines = new FlatMapOperator<>(
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
        files_lines.setName("files giving lines");

        FlatMapOperator<String, String> words = new FlatMapOperator<>(
                line -> Arrays.asList(line.split("\\W+")),
                String.class,
                String.class
        );
        words.setName("words");

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

        TextFileSink<Tuple2> sink = new TextFileSink<>(outputFileUrl, Tuple2.class);
        sink.setName("Collect result");


        source.connectTo(0, files_lines, 0);
        files_lines.connectTo(0, words,0);
        words.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}