package org.qcri.rheem.tests;

import org.qcri.rheem.api.DataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.basic.operators.DoWhileOperator;
import org.qcri.rheem.basic.operators.FilterOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.GlobalMaterializedGroupOperator;
import org.qcri.rheem.basic.operators.IntersectOperator;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.LoopOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.MapPartitionsOperator;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.basic.operators.SampleOperator;
import org.qcri.rheem.basic.operators.SortOperator;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.basic.operators.UnionAllOperator;
import org.qcri.rheem.basic.operators.ZipWithIdOperator;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.operators.SparkShufflePartitionSampleOperator;
import org.qcri.rheem.sqlite3.Sqlite3;
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Provides plans that can be used for integration testing..
 */
public class RheemPlans {

    public static final URI FILE_SOME_LINES_TXT = createUri("/some-lines.txt");

    public static final URI FILE_OTHER_LINES_TXT = createUri("/other-lines.txt");

    public static final URI ULYSSES_TXT = createUri("/ulysses.txt");

    public static final URI FILE_WITH_KEY_1 = createUri("/lines-with-key1.txt");

    public static final URI FILE_WITH_KEY_2 = createUri("/lines-with-key2.txt");

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    /**
     * Creates a {@link RheemPlan} consisting of a {@link TextFileSource} and a {@link LocalCallbackSink}.
     */
    public static RheemPlan readWrite(URI inputFileUri, List<String> collector) {
        TextFileSource textFileSource = new TextFileSource(inputFileUri.toString());
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, String.class);
        textFileSource.connectTo(0, sink, 0);
        return new RheemPlan(sink);
    }

    /**
     * Creates a {@link RheemPlan} consisting of a {@link TextFileSource}, a {@link MapOperator} (performs
     * {@link String#toUpperCase()}), and a {@link LocalCallbackSink}.
     */
    public static RheemPlan readTransformWrite(URI inputFileUri) {
        TextFileSource textFileSource = new TextFileSource(inputFileUri.toString());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                String::toUpperCase, String.class, String.class
        );
        textFileSource.connectTo(0, reverseOperator, 0);
        LocalCallbackSink<String> stdoutSink = LocalCallbackSink.createStdoutSink(String.class);
        reverseOperator.connectTo(0, stdoutSink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);
        return rheemPlan;
    }

    /**
     * Creates a {@link RheemPlan} with two {@link CollectionSource}s and two {@link LocalCallbackSink}s. Both sources
     * go into a {@link UnionAllOperator} and for the first {@link LocalCallbackSink}, the data quanta are routed
     * via a {@link MapOperator} that applies {@link String#toUpperCase()}.
     */
    public static RheemPlan multiSourceMultiSink(List<String> inputList1, List<String> inputList2,
                                                 List<String> collector1, List<String> collector2) {
        CollectionSource<String> source1 = new CollectionSource<>(inputList1, String.class);
        source1.setName("source1");
        CollectionSource<String> source2 = new CollectionSource<>(inputList2, String.class);
        source2.setName("source2");

        UnionAllOperator<String> coalesceOperator = new UnionAllOperator<>(String.class);
        coalesceOperator.setName("source1+2");
        source1.connectTo(0, coalesceOperator, 0);
        source2.connectTo(0, coalesceOperator, 1);

        MapOperator<String, String> uppercaseOperator = new MapOperator<>(
                String::toUpperCase, String.class, String.class
        );
        uppercaseOperator.setName("uppercase");
        coalesceOperator.connectTo(0, uppercaseOperator, 0);

        LocalCallbackSink<String> sink1 = LocalCallbackSink.createCollectingSink(collector1, String.class);
        sink1.setName("sink1");
        uppercaseOperator.connectTo(0, sink1, 0);

        LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(collector2, String.class);
        sink2.setName("sink2");
        coalesceOperator.connectTo(0, sink2, 0);

        return new RheemPlan(sink1, sink2);
    }

    /**
     * Creates a {@link RheemPlan} with two {@link CollectionSource}s and two {@link LocalCallbackSink}s. Both sources
     * go into a {@link UnionAllOperator}. Then, the data flow diverges again and to the branches one {@link MapOperator}
     * is applied with {@link String#toUpperCase()} and {@link String#toLowerCase()}. Finally, the both branches
     * are united via another {@link UnionAllOperator}, which is in turn consumed by the two {@link LocalCallbackSink}s.
     */
    public static RheemPlan multiSourceHoleMultiSink(List<String> inputList1, List<String> inputList2,
                                                     List<String> collector1, List<String> collector2) {

        CollectionSource<String> source1 = new CollectionSource<>(inputList1, String.class);
        source1.setName("source1");
        CollectionSource<String> source2 = new CollectionSource<>(inputList2, String.class);
        source2.setName("source2");

        UnionAllOperator<String> coalesceOperator1 = new UnionAllOperator<>(String.class);
        coalesceOperator1.setName("union1");
        source1.connectTo(0, coalesceOperator1, 0);
        source2.connectTo(0, coalesceOperator1, 1);

        MapOperator<String, String> lowerCaseOperator = new MapOperator<>(
                String::toLowerCase, String.class, String.class
        );
        lowerCaseOperator.setName("toLowerCase");
        coalesceOperator1.connectTo(0, lowerCaseOperator, 0);

        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                String::toUpperCase, String.class, String.class
        );
        upperCaseOperator.setName("toUpperCase");
        coalesceOperator1.connectTo(0, upperCaseOperator, 0);

        UnionAllOperator<String> coalesceOperator2 = new UnionAllOperator<>(String.class);
        coalesceOperator2.setName("union2");
        lowerCaseOperator.connectTo(0, coalesceOperator2, 0);
        upperCaseOperator.connectTo(0, coalesceOperator2, 1);

        LocalCallbackSink<String> sink1 = LocalCallbackSink.createCollectingSink(collector1, String.class);
        sink1.setName("sink1");
        coalesceOperator2.connectTo(0, sink1, 0);

        LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(collector2, String.class);
        sink2.setName("sink2");
        coalesceOperator2.connectTo(0, sink2, 0);

        return new RheemPlan(sink1, sink2);
    }

    /**
     * Creates a {@link RheemPlan} with a {@link TextFileSource}, a {@link SortOperator}, a {@link MapOperator},
     * a {@link DistinctOperator}, a {@link CountOperator}, and finally a {@link LocalCallbackSink} (stdout).
     */
    public static RheemPlan diverseScenario1(URI inputFileUri) {

        // Build a Rheem plan.
        TextFileSource textFileSource = new TextFileSource(inputFileUri.toString());
        textFileSource.setName("Load input file");
        SortOperator<String, String> sortOperator = new SortOperator<>(in->in, String.class, String.class);
        sortOperator.setName("Sort lines");
        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                String::toUpperCase, String.class, String.class
        );
        upperCaseOperator.setName("To uppercase");
        DistinctOperator<String> distinctLinesOperator = new DistinctOperator<>(String.class);
        distinctLinesOperator.setName("Make lines distinct");
        CountOperator<String> countLinesOperator = new CountOperator<>(String.class);
        countLinesOperator.setName("Count lines");
        LocalCallbackSink<Long> stdoutSink = LocalCallbackSink.createStdoutSink(Long.class);
        stdoutSink.setName("Print count");

        textFileSource.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, countLinesOperator, 0);
        countLinesOperator.connectTo(0, stdoutSink, 0);

        return new RheemPlan(stdoutSink);
    }

    /**
     * Creates a {@link RheemPlan} with two {@link TextFileSource}s, of which the first goes through a {@link FilterOperator}
     * Then, they are unioned in a {@link UnionAllOperator}, go through a {@link SortOperator}, a {@link MapOperator}
     * (applies {@link String#toUpperCase()}), {@link DistinctOperator}, and finally a {@link LocalCallbackSink} (stdout).
     */
    public static RheemPlan diverseScenario2(URI inputFileUri1, URI inputFileUri2) throws URISyntaxException {
        // Build a Rheem plan.
        TextFileSource textFileSource1 = new TextFileSource(inputFileUri1.toString());
        TextFileSource textFileSource2 = new TextFileSource(inputFileUri2.toString());
        FilterOperator<String> noCommaOperator = new FilterOperator<>(s -> !s.contains(","), String.class);
        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                String::toUpperCase, String.class, String.class
        );
        UnionAllOperator<String> unionOperator = new UnionAllOperator<>(String.class);
        SortOperator<String, String> sortOperator = new SortOperator<>(r->r, String.class, String.class);
        DistinctOperator<String> distinctLinesOperator = new DistinctOperator<>(String.class);
        LocalCallbackSink<String> stdoutSink = LocalCallbackSink.createStdoutSink(String.class);

        // Read from file 1, remove commas, union with file 2, sort, upper case, then remove duplicates and output.
        textFileSource1.connectTo(0, noCommaOperator, 0);
        textFileSource2.connectTo(0, unionOperator, 0);
        noCommaOperator.connectTo(0, unionOperator, 1);
        unionOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, stdoutSink, 0);

        return new RheemPlan(stdoutSink);
    }

    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource} that is fed into a {@link LoopOperator}. It will
     * then {@code k} times map each value to {@code 2n} and {@code 2n+1}. Finally, the outcome of the loop is
     * collected in the {@code collector}.
     */
    public static RheemPlan simpleLoop(final int numIterations, Collection<Integer> collector, final int... values)
            throws URISyntaxException {
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        CollectionSource<Integer> convergenceSource = new CollectionSource<>(RheemArrays.asList(0), Integer.class);
        convergenceSource.setName("convergenceSource");


        LoopOperator<Integer, Integer> loopOperator = new LoopOperator<>(DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) collection ->
                        collection.iterator().next() >= numIterations,
                numIterations
        );
        loopOperator.setName("loop");
        loopOperator.initialize(source, convergenceSource);

        FlatMapOperator<Integer, Integer> stepOperator = new FlatMapOperator<>(
                val -> Arrays.asList(2 * val, 2 * val + 1),
                Integer.class,
                Integer.class
        );
        stepOperator.setName("step");

        MapOperator<Integer, Integer> counter = new MapOperator<>(
                new TransformationDescriptor<>(n -> n + 1, Integer.class, Integer.class)
        );
        counter.setName("counter");
        loopOperator.beginIteration(stepOperator, counter);
        loopOperator.endIteration(stepOperator, counter);

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(collector, Integer.class);
        sink.setName("sink");
        loopOperator.outputConnectTo(sink);

        // Create the RheemPlan.
        return new RheemPlan(sink);
    }

    /**
     * Creates a {@link RheemPlan} that goes through a loop thereby incorporating the iteration number.
     */
    public static Collection<Integer> loopWithIterationNumber(RheemContext rheemContext,
                                                              final int maxValue,
                                                              final int expectedNumIterations,
                                                              final int... values) {
        return new JavaPlanBuilder(rheemContext)
                .loadCollection(RheemArrays.asList(values)).withName("Load values")
                .doWhile(
                        vals -> {
                            for (Integer val : vals) {
                                if (val >= maxValue) return true;
                            }
                            return false;
                        },
                        loopHead -> {
                            DataQuantaBuilder<?, Integer> newVals = loopHead
                                    .map(new IncreaseByIterationNumber())
                                    .withName("Increase by iteration number");
                            return new Tuple<>(
                                    newVals.map(x -> x).withName("Identity 1").withOutputClass(Integer.class),
                                    newVals.map(x -> x).withName("Identity 2").withOutputClass(Integer.class)
                            );
                        }
                ).withExpectedNumberOfIterations(expectedNumIterations).withConditionClass(Integer.class)
                .collect();
    }

    /**
     * Increases all incoming {@link Integer}s by the current iteration number.
     */
    public static class IncreaseByIterationNumber
            implements FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer> {

        private int increment;

        @Override
        public void open(ExecutionContext ctx) {
            this.increment = ctx.getCurrentIteration();
        }

        @Override
        public Integer apply(Integer integer) {
            return integer + this.increment;
        }
    }

    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource} that is fed into a {@link SampleOperator}. It will
     * then map each value to its double and output the results in the {@code collector}.
     */
    public static RheemPlan simpleSample(int sampleSize, Collection<Integer> collector, final int... values)
            throws URISyntaxException {
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        SampleOperator<Integer> sampleOperator = new SampleOperator<>(
                sampleSize, DataSetType.createDefault(Integer.class), SampleOperator.Methods.RANDOM, SampleOperator.randomSeed()
        );
        sampleOperator.setName("sample");

        MapOperator<Integer, Integer> mapOperator = new MapOperator<>(n -> 2 * n, Integer.class, Integer.class);
        mapOperator.setName("map");

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(collector, Integer.class);
        sink.setName("sink");

        source.connectTo(0, sampleOperator, 0);
        sampleOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, sink, 0);

        // Create the RheemPlan.
        return new RheemPlan(sink);
    }

    public static RheemPlan sampleInLoop(int sampleSize, int iterations, Collection<Integer> collector, final int... inputValues) {

        // Prepare test data.
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(inputValues), Integer.class);
        source.setName("source");

        CollectionSource<Integer> convergenceSource = new CollectionSource<>(RheemArrays.asList(0), Integer.class);
        convergenceSource.setName("convergenceSource");


        LoopOperator<Integer, Integer> loopOperator = new LoopOperator<>(DataSetType.createDefault(Integer.class),
                DataSetType.createDefault(Integer.class),
                (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) collection ->
                        collection.iterator().next() >= iterations, iterations
        );
        loopOperator.setName("loop");
        loopOperator.initialize(source, convergenceSource);

        // Build the sample operator.
        SparkShufflePartitionSampleOperator<Integer> sampleOperator =
                new SparkShufflePartitionSampleOperator<>(
                        iterationNumber -> sampleSize,
                        DataSetType.createDefaultUnchecked(Integer.class),
                        iterationNumber -> 42 + iterationNumber
                );
        sampleOperator.setDatasetSize(10);
        sampleOperator.setName("sample");

        MapOperator<Integer, Integer> counter = new MapOperator<>(
                new TransformationDescriptor<>(n -> n + 1, Integer.class, Integer.class)
        );
        counter.setName("counter");
        loopOperator.beginIteration(sampleOperator, counter);
        loopOperator.endIteration(sampleOperator, counter);

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(collector, Integer.class);
        sink.setName("sink");
        loopOperator.outputConnectTo(sink);

        return new RheemPlan(sink);
    }

    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource} that is fed into a {@link GlobalMaterializedGroupOperator}.
     * It will then push the results in the {@code collector}.
     */
    public static RheemPlan globalMaterializedGroup(Collection<Iterable<Integer>> collector, final int... values)
            throws URISyntaxException {
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        GlobalMaterializedGroupOperator<Integer> globalMaterializedGroupOperator =
                new GlobalMaterializedGroupOperator<>(Integer.class);
        globalMaterializedGroupOperator.setName("group");

        LocalCallbackSink<Iterable<Integer>> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createGrouped(Integer.class)
        );
        sink.setName("sink");

        source.connectTo(0, globalMaterializedGroupOperator, 0);
        globalMaterializedGroupOperator.connectTo(0, sink, 0);

        // Create the RheemPlan.
        return new RheemPlan(sink);
    }


    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource} that is fed into a {@link RepeatOperator}.
     * The input values will be incremented by 1 n times.
     * It will then push the results in the {@code collector}.
     */
    public static RheemPlan repeat(Collection<Integer> collector, int numIterations, final int... values) {
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        RepeatOperator<Integer> repeat = new RepeatOperator<>(numIterations, Integer.class);
        repeat.setName("repeat");

        MapOperator<Integer, Integer> increment = new MapOperator<>(
                i -> i + 1, Integer.class, Integer.class
        );
        increment.setName("increment");

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefault(Integer.class)
        );
        sink.setName("sink");

        repeat.initialize(source, 0);
        repeat.beginIteration(increment, 0);
        repeat.endIteration(increment, 0);
        repeat.connectFinalOutputTo(sink, 0);

        return new RheemPlan(sink);
    }


    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource} that is fed into a {@link ZipWithIdOperator}.
     * It will then push the results in the {@code collector}.
     */
    public static RheemPlan zipWithId(Collection<Long> collector, final int... values)
            throws URISyntaxException {
        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        ZipWithIdOperator<Integer> zipWithId = new ZipWithIdOperator<>(Integer.class);
        zipWithId.setName("zipWithId");

        MapOperator<Tuple2<Long, Integer>, Long> stripValue = new MapOperator<>(
                tuple -> tuple.field0, ReflectionUtils.specify(Tuple2.class), Long.class
        );
        stripValue.setName("stripValue");

        DistinctOperator<Long> distinctIds = new DistinctOperator<>(Long.class);
        distinctIds.setName("distinctIds");

        CountOperator<Long> count = new CountOperator<>(Long.class);
        count.setName("count");

        LocalCallbackSink<Long> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefault(Long.class)
        );
        sink.setName("sink");

        source.connectTo(0, zipWithId, 0);
        zipWithId.connectTo(0, stripValue, 0);
        stripValue.connectTo(0, distinctIds, 0);
        distinctIds.connectTo(0, count, 0);
        count.connectTo(0, sink, 0);

        // Create the RheemPlan.
        return new RheemPlan(sink);
    }


    /**
     * Creates a {@link RheemPlan} with a {@link CollectionSource}. The data quanta are separated into negative and
     * non-negative. Then, their squares are intersected using the {@link IntersectOperator}. The result is
     * pushed to the {@code collector}.
     */
    public static RheemPlan intersectSquares(Collection<Integer> collector, final int... values)
            throws URISyntaxException {

        CollectionSource<Integer> source = new CollectionSource<>(RheemArrays.asList(values), Integer.class);
        source.setName("source");

        FilterOperator<Integer> filterNegative = new FilterOperator<>(i -> i < 0, Integer.class);
        filterNegative.setName("filterNegative");
        source.connectTo(0, filterNegative, 0);

        MapOperator<Integer, Integer> squareNegative = new MapOperator<>(i -> i * i, Integer.class, Integer.class);
        squareNegative.setName("squareNegative");
        filterNegative.connectTo(0, squareNegative, 0);

        FilterOperator<Integer> filterPositive = new FilterOperator<>(i -> i >= 0, Integer.class);
        filterPositive.setName("filterPositive");
        source.connectTo(0, filterPositive, 0);

        MapOperator<Integer, Integer> squarePositive = new MapOperator<>(i -> i * i, Integer.class, Integer.class);
        squarePositive.setName("squarePositive");
        filterPositive.connectTo(0, squarePositive, 0);

        IntersectOperator<Integer> intersect = new IntersectOperator<>(Integer.class);
        intersect.setName("intersect");
        squarePositive.connectTo(0, intersect, 1);
        squareNegative.connectTo(0, intersect, 0);

        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(
                collector,
                Integer.class
        );
        sink.setName("sink");
        intersect.connectTo(0, sink, 0);

        // Create the RheemPlan.
        return new RheemPlan(sink);
    }

    /**
     * Creates a cross-community PageRank Rheem plan, that incorporates the {@link PageRankOperator}.
     */
    public static RheemPlan pageRankWithDictionaryCompression(Collection<Tuple2<Character, Float>> pageRankCollector) {
        // Get some graph data. Use the example from Wikipedia: https://en.wikipedia.org/wiki/PageRank
        Collection<char[]> adjacencies = Arrays.asList(
                new char[]{'B', 'C'},
                new char[]{'C', 'B'},
                new char[]{'D', 'A', 'B'},
                new char[]{'E', 'B', 'D', 'F'},
                new char[]{'F', 'B', 'E'},
                new char[]{'G', 'B', 'E'},
                new char[]{'H', 'B', 'E'},
                new char[]{'I', 'B', 'E'},
                new char[]{'J', 'E'},
                new char[]{'K', 'E'}
        );

        // Create a RheemPlan:

        // Load the adjacency list.
        final CollectionSource<char[]> adjacencySource = new CollectionSource<>(adjacencies, char[].class);
        adjacencySource.setName("adjacency source");

        // Split the adjacency list into an edge list.
        FlatMapOperator<char[], Tuple2<Character, Character>> adjacencySplitter = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (adjacence) -> {
                            List<Tuple2<Character, Character>> result = new ArrayList<>(adjacence.length - 1);
                            for (int i = 1; i < adjacence.length; i++) {
                                result.add(new Tuple2<>(adjacence[0], adjacence[i]));
                            }
                            return result;
                        },
                        char[].class,
                        ReflectionUtils.specify(Tuple2.class))
        );
        adjacencySplitter.setName("adjacency splitter");
        adjacencySource.connectTo(0, adjacencySplitter, 0);

        // Extract the vertices from the edge list.
        FlatMapOperator<Tuple2<Character, Character>, Character> vertexSplitter = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (edge) -> {
                            List<Character> vertices = new ArrayList<>(2);
                            vertices.add(edge.field0);
                            vertices.add(edge.field1);
                            return vertices;
                        },
                        ReflectionUtils.specify(Tuple2.class),
                        Character.class
                )
        );
        vertexSplitter.setName("vertex splitter");
        adjacencySplitter.connectTo(0, vertexSplitter, 0);

        // Find the distinct vertices.
        DistinctOperator<Character> vertexCanonicalizer = new DistinctOperator<>(Character.class);
        vertexCanonicalizer.setName("vertex canonicalizer");
        vertexSplitter.connectTo(0, vertexCanonicalizer, 0);

        // Assign an ID to each distinct vertex.
        ZipWithIdOperator<Character> zipWithId = new ZipWithIdOperator<>(Character.class);
        zipWithId.setName("zip with ID");
        vertexCanonicalizer.connectTo(0, zipWithId, 0);

        // Base the edge list on vertex IDs.
        MapOperator<Tuple2<Character, Character>, Tuple2<Long, Long>> translate = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Character, Character>, Tuple2<Long, Long>>() {

                            private Map<Character, Long> dictionary;

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.dictionary = ctx.<Tuple2<Long, Character>>getBroadcast("vertex IDs").stream()
                                        .collect(Collectors.toMap(Tuple2::getField1, Tuple2::getField0));
                            }

                            @Override
                            public Tuple2<Long, Long> apply(Tuple2<Character, Character> in) {
                                return new Tuple2<>(this.dictionary.get(in.field0), this.dictionary.get(in.field1));
                            }
                        },
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                )
        );
        translate.setName("translate");
        adjacencySplitter.connectTo(0, translate, 0);
        zipWithId.broadcastTo(0, translate, "vertex IDs");

        // Run the PageRank algorithm.
        PageRankOperator pageRank = new PageRankOperator(20);
        pageRank.setName("PageRank");
        translate.connectTo(0, pageRank, 0);

        // Back-translate the page ranks.
        MapOperator<Tuple2<Long, Float>, Tuple2<Character, Float>> backtranslate = new MapOperator<>(
                new TransformationDescriptor<>(
                        new FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Long, Float>, Tuple2<Character, Float>>() {

                            private Map<Long, Character> dictionary;

                            @Override
                            public void open(ExecutionContext ctx) {
                                this.dictionary = ctx.<Tuple2<Long, Character>>getBroadcast("vertex IDs").stream()
                                        .collect(Collectors.toMap(Tuple2::getField0, Tuple2::getField1));
                            }

                            @Override
                            public Tuple2<Character, Float> apply(Tuple2<Long, Float> in) {
                                return new Tuple2<>(this.dictionary.get(in.field0), in.field1);
                            }
                        },
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                )
        );
        backtranslate.setName("bracktranslate");
        pageRank.connectTo(0, backtranslate, 0);
        zipWithId.broadcastTo(0, backtranslate, "vertex IDs");

        LocalCallbackSink callbackSink = LocalCallbackSink.createCollectingSink(
                pageRankCollector,
                DataSetType.<Tuple2<Character, Float>>createDefaultUnchecked(Tuple2.class)
        );
        callbackSink.setName("sink");
        backtranslate.connectTo(0, callbackSink, 0);

        return new RheemPlan(callbackSink);
    }

    public static Map<Character, Float> pageRankWithDictionaryCompressionSolution() {
        return Stream.of(
                new Tuple2<>('A', 0.033f),
                new Tuple2<>('B', 0.384f),
                new Tuple2<>('C', 0.343f),
                new Tuple2<>('D', 0.039f),
                new Tuple2<>('E', 0.081f),
                new Tuple2<>('F', 0.039f),
                new Tuple2<>('G', 0.016f),
                new Tuple2<>('H', 0.016f),
                new Tuple2<>('I', 0.016f),
                new Tuple2<>('J', 0.016f),
                new Tuple2<>('K', 0.016f)
        ).collect(Collectors.toMap(Tuple2::getField0, Tuple2::getField1));
    }

    /**
     * Feeds the {@code edges} into a {@link PageRankOperator} and collects the page ranks in the {@code collector}.
     *
     * @return a {@link RheemPlan} implementing the above described
     */
    public static RheemPlan pageRank(Collection<Tuple2<Long, Long>> edges,
                                     Collection<Tuple2<Long, Float>> collector) {
        CollectionSource<Tuple2<Long, Long>> source = new CollectionSource<>(
                edges, ReflectionUtils.specify(Tuple2.class)
        );
        source.setName("source");

        PageRankOperator pageRank = new PageRankOperator(20);
        pageRank.setName("pageRank");
        source.connectTo(0, pageRank, 0);

        final LocalCallbackSink<Tuple2<Long, Float>> sink =
                LocalCallbackSink.createCollectingSink(collector, ReflectionUtils.specify(Tuple2.class));
        pageRank.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    /**
     * Creates and executed a {@link RheemPlan} that counts the number of even and odd numbers using a
     * {@link MapPartitionsOperator} to pre-aggregate partitions.
     *
     * @param rheemContext provide the execution environment
     * @param inputValues  that should be dissected and counted
     */
    public static Collection<Tuple2<String, Integer>> mapPartitions(RheemContext rheemContext, int... inputValues) {
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Execute the job.
        return builder
                .loadCollection(RheemArrays.asList(inputValues))
                .mapPartitions(partition -> {
                    int numEvens = 0, numOdds = 0;
                    for (Integer value : partition) {
                        if ((value & 1) == 0) numEvens++;
                        else numOdds++;
                    }
                    return Arrays.asList(
                            new Tuple2<>("odd", numOdds),
                            new Tuple2<>("even", numEvens)
                    );
                })
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .collect();

    }

    /**
     * Same as scenarion2 but repeat 10 times before output.
     */
    public static RheemPlan diverseScenario3(URI inputFileUri1, URI inputFileUri2) throws URISyntaxException {
        // Build a Rheem plan.
        TextFileSource textFileSource1 = new TextFileSource(inputFileUri1.toString());
        textFileSource1.setName("Source 1");
        TextFileSource textFileSource2 = new TextFileSource(inputFileUri2.toString());
        textFileSource2.setName("Source 2");
        FilterOperator<String> noCommaOperator = new FilterOperator<>(s -> !s.contains(","), String.class);
        noCommaOperator.setName("Filter comma");
        UnionAllOperator<String> unionOperator = new UnionAllOperator<>(String.class);
        unionOperator.setName("Union");
        LocalCallbackSink<String> stdoutSink = LocalCallbackSink.createStdoutSink(String.class);
        stdoutSink.setName("Print");
        SortOperator<String, String> sortOperator = new SortOperator<>(r->r, String.class, String.class);
        sortOperator.setName("Sort");
        CountOperator<String> countLines = new CountOperator<>(String.class);
        countLines.setName("Count");
        DoWhileOperator<String, Long> loopOperator = new DoWhileOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(Long.class),
                integers -> integers.iterator().next() > 100,
                100
        );
        loopOperator.setName("Do while");
        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                new TransformationDescriptor<>(String::toUpperCase, String.class, String.class)
        );
        upperCaseOperator.setName("To uppercase");
        FilterOperator<String> dummyFilter = new FilterOperator<>(str -> true, String.class);
        dummyFilter.setName("Dummy filter");

        // Read from file 1, remove commas, union with file 2, sort, upper case, then remove duplicates and output.
        loopOperator.initialize(textFileSource1, 0);
        loopOperator.beginIteration(noCommaOperator, 0);
        textFileSource2.connectTo(0, unionOperator, 0);
        noCommaOperator.connectTo(0, unionOperator, 1);
        unionOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, countLines, 0);
        sortOperator.connectTo(0, dummyFilter, 0);
        loopOperator.endIteration(dummyFilter, 0, countLines, 0);
        loopOperator.outputConnectTo(upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, stdoutSink, 0);

        // Create the RheemPlan.
        return new RheemPlan(stdoutSink);
    }

    public static Integer increment(Integer k) {
        if (k == null) {
            return 1;
        } else {
            return k++;
        }
    }

    public static String concat9(String k) {
        return k.concat("9");
    }

    /**
     * Simple counter loop .
     */
    public static RheemPlan diverseScenario4(URI inputFileUri1, URI inputFileUri2) throws URISyntaxException {
        // Build a Rheem plan.
        TextFileSource textFileSource1 = new TextFileSource(inputFileUri1.toString());
        textFileSource1.setName("file1");
        TextFileSource textFileSource2 = new TextFileSource(inputFileUri2.toString());
        textFileSource2.setName("file2");
        MapOperator<Integer, Integer> counter = new MapOperator<>(
                new TransformationDescriptor<>(n -> n + 1, Integer.class, Integer.class)
        );
        counter.setName("counter");
        UnionAllOperator<String> unionOperator = new UnionAllOperator<>(String.class);
        unionOperator.setName("union");
        LocalCallbackSink<String> stdoutSink = LocalCallbackSink.createStdoutSink(String.class);
        stdoutSink.setName("stdout");

        LoopOperator<String, Integer> loopOperator = new LoopOperator<>(DataSetType.createDefault(String.class),
                DataSetType.createDefault(Integer.class),
                (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) collection ->
                        collection.iterator().next() >= 10,
                10
        );
        loopOperator.setName("loop");

        // Union 10 times then output
        loopOperator.initialize(textFileSource1, CollectionSource.singleton(0, Integer.class));
        loopOperator.beginIteration(unionOperator, counter);
        textFileSource2.connectTo(0, unionOperator, 1);
        loopOperator.endIteration(unionOperator, counter);
        loopOperator.outputConnectTo(stdoutSink, 0);

        // Create the RheemPlan.
        return new RheemPlan(stdoutSink);
    }

    /**
     * Prepair a SQLite3 database for the {@code sqlite3Scenario*} methods.
     *
     * @param configuration designates the location of the database
     * @throws SQLException
     */
    public static void prepareSqlite3Scenarios(Configuration configuration) throws SQLException {
        try (Connection connection = Sqlite3.platform()
                .createDatabaseDescriptor(configuration)
                .createJdbcConnection()) {
            final Statement statement = connection.createStatement();
            statement.addBatch("DROP TABLE IF EXISTS customer;");
            statement.addBatch("CREATE TABLE customer (name TEXT, age INT);");
            statement.addBatch("INSERT INTO customer VALUES ('John', 20)");
            statement.addBatch("INSERT INTO customer VALUES ('Timmy', 16)");
            statement.addBatch("INSERT INTO customer VALUES ('Evelyn', 35)");
            statement.executeBatch();
        }
    }

    public static List<Record> getSqlite3Customers() {
        return Arrays.asList(
                new Record("John", 20),
                new Record("Timmy", 16),
                new Record("Evelyn", 35)
        );
    }

    public static RheemPlan sqlite3Scenario1(Collection<Record> collector) {
        Sqlite3TableSource customers = new Sqlite3TableSource("customer");
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);
        customers.connectTo(0, sink, 0);
        return new RheemPlan(sink);

    }

    public static RheemPlan sqlite3Scenario2(Collection<Record> collector) {
        Sqlite3TableSource customers = new Sqlite3TableSource("customer", "name", "age");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> (Integer) record.getField(1) >= 18,
                        Record.class
                ).withSqlImplementation("age >= 18"),
                DataSetType.createDefault(Record.class)
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);

        customers.connectTo(0, filter, 0);
        filter.connectTo(0, sink, 0);

        return new RheemPlan(sink);

    }

    public static RheemPlan sqlite3Scenario3(Collection<Record> collector) {
        Sqlite3TableSource customers = new Sqlite3TableSource("customer", "name", "age");
        FilterOperator<Record> filter = new FilterOperator<>(
                new PredicateDescriptor<>(
                        (PredicateDescriptor.SerializablePredicate<Record>) record -> (Integer) record.getField(1) >= 18,
                        Record.class
                ).withSqlImplementation("age >= 18"),
                customers.getType()
        );
        MapOperator<Record, Record> projection = MapOperator.createProjection(
                (RecordType) filter.getOutputType().getDataUnitType(),
                "name"
        );
        LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);

        customers.connectTo(0, filter, 0);
        filter.connectTo(0, projection, 0);
        projection.connectTo(0, sink, 0);

        return new RheemPlan(sink);

    }


}


