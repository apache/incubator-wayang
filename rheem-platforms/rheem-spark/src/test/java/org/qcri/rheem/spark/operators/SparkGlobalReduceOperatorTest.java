package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkGlobalReduceOperator}.
 */
public class SparkGlobalReduceOperatorTest {

    @Test
    public void testExecution() {
//        // Prepare test data.
//        Stream<Integer> inputStream = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).stream();
//
//        // Build the reduce operator.
//        JavaGlobalReduceOperator<Integer> globalReduce =
//                new JavaGlobalReduceOperator<>(
//                        DataSetType.createDefaultUnchecked(Tuple2.class),
//                        new ReduceDescriptor<>(
//                                DataUnitType.createGrouped(Integer.class),
//                                DataUnitType.createBasic(Integer.class),
//                                (a, b) -> a + b
//                        )
//                );
//
//        // Execute the reduce operator.
//        final Stream[] outputStreams = globalReduce.evaluate(new Stream[]{inputStream}, new FunctionCompiler());
//
//        // Verify the outcome.
//        Assert.assertEquals(1, outputStreams.length);
//        final List<Integer> result =
//                ((Stream<Integer>) outputStreams[0]).collect(Collectors.toList());
//        Assert.assertEquals(1, result.size());
//        Assert.assertEquals(Integer.valueOf((10 + 1) * (10 / 2)), result.get(0)); // Props to Gauss!

    }

    @Test
    public void testExecutionWithoutData() {
//        // Prepare test data.
//        Stream<Integer> inputStream = Arrays.<Integer>asList().stream();
//
//        // Build the reduce operator.
//        JavaGlobalReduceOperator<Integer> globalReduce =
//                new JavaGlobalReduceOperator<>(
//                        DataSetType.createDefaultUnchecked(Tuple2.class),
//                        new ReduceDescriptor<>(
//                                DataUnitType.createGrouped(Integer.class),
//                                DataUnitType.createBasic(Integer.class),
//                                (a, b) -> a + b
//                        )
//                );
//
//        // Execute the reduce operator.
//        final Stream[] outputStreams = globalReduce.evaluate(new Stream[]{inputStream}, new FunctionCompiler());
//
//        // Verify the outcome.
//        Assert.assertEquals(1, outputStreams.length);
//        final List<Integer> result =
//                ((Stream<Integer>) outputStreams[0]).collect(Collectors.toList());
//        Assert.assertEquals(0, result.size());

    }
}
