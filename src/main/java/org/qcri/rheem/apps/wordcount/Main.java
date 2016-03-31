package org.qcri.rheem.apps.wordcount;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
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
    public static RheemPlan createRheemPlan(String inputFileUrl) throws URISyntaxException, IOException {
        // Assignment mode: none.

        TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.addTargetPlatform(JavaPlatform.getInstance());
        textFileSource.addTargetPlatform(SparkPlatform.getInstance());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasic(Tuple2.class),
                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        ), DataSetType.createDefaultUnchecked(Tuple2.class)
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
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

            RheemPlan rheemPlan = createRheemPlan(args[1]);

            RheemContext rheemContext = new RheemContext();
            for (String platform : args[0].split(",")) {
                switch (platform) {
                    case "java":
                        rheemContext.register(JavaPlatform.getInstance());
                        break;
                    case "spark":
                        rheemContext.register(SparkPlatform.getInstance());
                        break;
                    default:
                        System.err.format("Unknown platform: \"%s\"\n", platform);
                        System.exit(3);
                        return;
                }
            }

            rheemContext.execute(rheemPlan);
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

}
