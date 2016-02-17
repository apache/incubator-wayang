package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.Counter;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.operators.JavaLocalCallbackSink;
import org.qcri.rheem.java.operators.JavaReduceByOperator;
import org.qcri.rheem.spark.operators.SparkFlatMapOperator;
import org.qcri.rheem.spark.operators.SparkMapOperator;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Word count test.
 */
public class WordCountIT {

    @Test
    public void testOnJava() throws URISyntaxException, IOException {

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                ));


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(Tuple2.class)
                ));


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasic(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(
                        DataUnitType.createGroupedUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        ((a, b) -> {
                            a.field1 += b.field1;
                            return a;
                        }))
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        // Have Rheem execute the plan.
        rheemContext.execute(new RheemPlan(sink));

        // Verify the plan result.
        Counter<String> counter = new Counter<>();
        List<Tuple2> correctResults = new ArrayList<>();
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        lines.stream()
                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .map(String::toLowerCase)
                .forEach(counter::increment);

        for (Map.Entry<String, Integer> countEntry : counter) {
            correctResults.add(new Tuple2<>(countEntry.getKey(), countEntry.getValue()));
        }
        Assert.assertTrue(results.size() == correctResults.size() && results.containsAll(correctResults) && correctResults.containsAll(results));
    }

    @Test
    public void testOnSpark() throws URISyntaxException, IOException {

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                ));


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(Tuple2.class)
                ));


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasic(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(
                        DataUnitType.createGroupedUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        ((a, b) -> {
                            a.field1 += b.field1;
                            return a;
                        }))
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);
        RheemPlan rheemPlan = new RheemPlan(sink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Verify the plan result.
        Counter<String> counter = new Counter<>();
        List<Tuple2> correctResults = new ArrayList<>();
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        lines.stream()
                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .map(String::toLowerCase)
                .forEach(counter::increment);

        for (Map.Entry<String, Integer> countEntry : counter) {
            correctResults.add(new Tuple2<>(countEntry.getKey(), countEntry.getValue()));
        }
        Assert.assertTrue(results.size() == correctResults.size() && results.containsAll(correctResults) && correctResults.containsAll(results));
    }

    @Test
    public void testOnSparkToJava() throws URISyntaxException, IOException {

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new SparkFlatMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                ));


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new SparkMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(Tuple2.class)
                ));


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new JavaReduceByOperator<>(
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasic(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(
                        DataUnitType.createGroupedUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        ((a, b) -> {
                            a.field1 += b.field1;
                            return a;
                        }))
        );


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = new JavaLocalCallbackSink<>(results::add, DataSetType.createDefault(Tuple2.class));

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);
        RheemPlan rheemPlan = new RheemPlan(sink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Verify the plan result.
        Counter<String> counter = new Counter<>();
        List<Tuple2> correctResults = new ArrayList<>();
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        lines.stream()
                .flatMap(line -> Arrays.stream(line.split("\\s+")))
                .map(String::toLowerCase)
                .forEach(counter::increment);

        for (Map.Entry<String, Integer> countEntry : counter) {
            correctResults.add(new Tuple2<>(countEntry.getKey(), countEntry.getValue()));
        }
        Assert.assertTrue(results.size() == correctResults.size() && results.containsAll(correctResults) && correctResults.containsAll(results));
    }


}
