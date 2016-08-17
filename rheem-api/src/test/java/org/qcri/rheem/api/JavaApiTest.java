package org.qcri.rheem.api;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
}
