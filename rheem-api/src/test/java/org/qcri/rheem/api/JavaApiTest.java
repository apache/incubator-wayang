package org.qcri.rheem.api;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.operators.JavaMapOperator;
import org.qcri.rheem.spark.Spark;

import java.util.*;

/**
 * Test suite for the Java API.
 */
public class JavaApiTest {

    @Test
    public void testMapReduce() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .globalReduce((a, b) -> a + b).withName("sum")
                .collect("testMapReduce()");

        Assert.assertEquals(RheemCollections.asSet(1 + 4 + 9 + 16), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testMapReduceBy() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .reduceBy(i -> i & 1, (a, b) -> a + b).withName("sum")
                .collect("testMapReduceBy()");

        Assert.assertEquals(RheemCollections.asSet(4 + 16, 1 + 9), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testBroadcast() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        List<Integer> offsetCollection = Collections.singletonList(-2);

        LoadCollectionDataQuantaBuilder<Integer> offsetDataQuanta = javaPlanBuilder
                .loadCollection(offsetCollection)
                .withName("load offset");

        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(new AddOffset("offset")).withName("add offset").withBroadcast(offsetDataQuanta, "offset")
                .collect("testBroadcast()");

        Assert.assertEquals(RheemCollections.asSet(-2, -1, 0, 1, 2), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testCustomOperatorShortCut() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        final List<Integer> inputValues = RheemArrays.asList(0, 1, 2, 3);

        // Build and execute a Rheem plan.
        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .<Integer>customOperator(new JavaMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new TransformationDescriptor<>(
                                i -> i + 2,
                                Integer.class, Integer.class
                        )
                )).withName("Add 2")
                .collect("testCustomOperatorShortCut()");

        // Check the outcome.
        final List<Integer> expectedOutputValues = RheemArrays.asList(2, 3, 4, 5);
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testWordCount() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Rheem plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceBy(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect("testWordCount()");

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = RheemCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testWordCountOnSparkAndJava() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Rheem plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceBy(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect("testWordCount()");

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = RheemCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testDoWhile() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = RheemArrays.asList(1, 2);

        // Build and execute a word count RheemPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .doWhile(
                        (PredicateDescriptor.SerializablePredicate<Collection<Integer>>) values -> values.stream().mapToInt(i -> i).sum() > 100,
                        start -> {
                            final GlobalReduceDataQuantaBuilder<Integer> sum =
                                    start.globalReduce((a, b) -> a + b).withName("sum");
                            return new Tuple<>(
                                    start.union(sum).withName("Old+new"),
                                    sum.map(x -> x).withName("Identity (hotfix)")
                            );
                        }
                ).withName("While <= 100")
                .collect("testDoWhile()");

        Set<Integer> expectedValues = RheemCollections.asSet(1, 2, 3, 6, 12, 24, 48, 96, 192);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    private static class AddOffset implements FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer> {

        private final String broadcastName;

        private int offset;

        public AddOffset(String broadcastName) {
            this.broadcastName = broadcastName;
        }

        @Override
        public void open(ExecutionContext ctx) {
            this.offset = RheemCollections.getSingle(ctx.<Integer>getBroadcast(this.broadcastName));
        }

        @Override
        public Integer apply(Integer input) {
            return input + this.offset;
        }
    }

    @Test
    public void testRepeat() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = RheemArrays.asList(1, 2);

        // Build and execute a word count RheemPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .repeat(3, start -> start
                        .globalReduce((a, b) -> a * b).withName("Multiply").withOutputType(DataSetType.createDefault(Integer.class))
                        .flatMap(v -> Arrays.asList(v, v + 1)).withName("Duplicate")
                ).withName("Repeat 3x")
                .collect("testRepeat()");

        Set<Integer> expectedValues = RheemCollections.asSet(42, 43);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }
}
