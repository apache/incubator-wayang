package org.qcri.rheem.apps.wordcount;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Example Rheem App that does a word count -- the Hello World of Map/Reduce-like systems.
 */
public class Main {

    /**
     * Creates the {@link RheemPlan} for the word count app.
     *
     * @param inputFileUrl the file whose words should be counted
     */
    public static RheemPlan createRheemPlan(String inputFileUrl, Collection<Tuple2<String, Integer>> collector) throws URISyntaxException, IOException {
        // Assignment mode: none.

        TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.setName("Load file");

        // for each line (input) output an iterator of the words
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


        // write results to a sink
        LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        sink.setName("Collect result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }

            List<Tuple2<String, Integer>> collector = new LinkedList<>();
            RheemPlan rheemPlan = createRheemPlan(args[1], collector);

            RheemContext rheemContext = new RheemContext();
            for (String platform : args[0].split(",")) {
                switch (platform) {
                    case "java":
                        rheemContext.register(Java.basicPlugin());
                        break;
                    case "spark":
                        rheemContext.register(Spark.basicPlugin());
                        break;
                    default:
                        System.err.format("Unknown platform: \"%s\"\n", platform);
                        System.exit(3);
                        return;
                }
            }

            rheemContext.execute(rheemPlan, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));

            collector.sort((t1, t2) -> Integer.compare(t2.field1, t1.field1));
            System.out.printf("Found %d words:\n", collector.size());
            collector.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

}
