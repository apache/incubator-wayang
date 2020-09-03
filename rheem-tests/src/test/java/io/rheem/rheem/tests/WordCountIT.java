package io.rheem.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.basic.data.Tuple2;
import io.rheem.rheem.basic.operators.FlatMapOperator;
import io.rheem.rheem.basic.operators.LocalCallbackSink;
import io.rheem.rheem.basic.operators.MapOperator;
import io.rheem.rheem.basic.operators.ReduceByOperator;
import io.rheem.rheem.basic.operators.TextFileSource;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.api.RheemContext;
import io.rheem.rheem.core.function.FlatMapDescriptor;
import io.rheem.rheem.core.function.ReduceDescriptor;
import io.rheem.rheem.core.function.TransformationDescriptor;
import io.rheem.rheem.core.optimizer.costs.DefaultLoadEstimator;
import io.rheem.rheem.core.optimizer.costs.LoadEstimator;
import io.rheem.rheem.core.plan.rheemplan.RheemPlan;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.types.DataUnitType;
import io.rheem.rheem.core.util.Counter;
import io.rheem.rheem.java.Java;
import io.rheem.rheem.java.operators.JavaLocalCallbackSink;
import io.rheem.rheem.java.operators.JavaReduceByOperator;
import io.rheem.rheem.spark.Spark;
import io.rheem.rheem.spark.operators.SparkFlatMapOperator;
import io.rheem.rheem.spark.operators.SparkMapOperator;
import io.rheem.rheem.spark.operators.SparkTextFileSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Word count integration test. Besides going through different {@link Platform} combinations, each test addresses a different
 * way of specifying the target {@link Platform}s.
 */
public class WordCountIT {

    @Test
    public void testOnJava() throws URISyntaxException, IOException {
        // Assignment mode: RheemContext.

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );
        flatMapOperator.getFunctionDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 670),
                LoadEstimator.createFallback(1, 1)
        );


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.<String>createBasic(String.class),
                        DataUnitType.<Tuple2<String, Integer>>createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        mapOperator.getFunctionDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 245),
                LoadEstimator.createFallback(1, 1)
        );


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
        reduceByOperator.getKeyDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 50),
                LoadEstimator.createFallback(1, 1)
        );
        reduceByOperator.getReduceDescriptor().setLoadEstimators(
                new DefaultLoadEstimator(1, 1, 0.9d, (inCards, outCards) -> inCards[0] * 350 + 500000),
                LoadEstimator.createFallback(1, 1)
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
        // Assignment mode: Job.

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );


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
        RheemContext rheemContext = new RheemContext();
        final Job job = rheemContext.createJob(null, rheemPlan);
        Spark.basicPlugin().configure(job.getConfiguration());
        job.execute();

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
        // Assignment mode: ExecutionOperators.

        // Instantiate Rheem and activate the backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(Spark.basicPlugin());
        rheemContext.register(Java.basicPlugin());

        TextFileSource textFileSource = new SparkTextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new SparkFlatMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split(" ")),
                        String.class,
                        String.class
                ));


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new SparkMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ));


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new JavaReduceByOperator<>(
                DataSetType.createDefaultUnchecked(Tuple2.class),
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(
                        ((a, b) -> {
                            a.field1 += b.field1;
                            return a;
                        }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                )
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

    @Test
    public void testOnJavaToSpark() throws URISyntaxException, IOException {
        // Assignment mode: Constraints.

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());
        textFileSource.addTargetPlatform(Java.platform());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );
        flatMapOperator.addTargetPlatform(Java.platform());
        flatMapOperator.setName("Split words");


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        mapOperator.addTargetPlatform(Spark.platform());
        mapOperator.setName("Create counters");


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
        reduceByOperator.addTargetPlatform(Spark.platform());
        reduceByOperator.setName("Add counters");


        // write results to a sink
        List<Tuple2> results = new ArrayList<>();
        LocalCallbackSink<Tuple2> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Tuple2.class));
        sink.addTargetPlatform(Spark.platform());

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);
        RheemPlan rheemPlan = new RheemPlan(sink);

        // Have Rheem execute the plan.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(Java.basicPlugin());
        rheemContext.register(Spark.basicPlugin());
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
        Assert.assertEquals(new HashSet<>(correctResults), new HashSet<>(results));
    }

    @Test
    public void testOnJavaAndSpark() throws URISyntaxException, IOException {
        // Assignment mode: none.

        TextFileSource textFileSource = new TextFileSource(RheemPlans.FILE_SOME_LINES_TXT.toString());
        textFileSource.addTargetPlatform(Java.platform());
        textFileSource.addTargetPlatform(Spark.platform());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList((String[]) line.split(" ")),
                        String.class,
                        String.class
                )
        );


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<String, Integer>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );


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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(Java.basicPlugin());
        rheemContext.register(Spark.basicPlugin());
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
