package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.channels.TestChannelExecutor;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test suite for {@link SparkMaterializedGroupByOperator}.
 */
public class SparkMaterializedGroupByOperatorTest extends SparkOperatorTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecution() {
        // Prepare test data.
        AtomicInteger counter = new AtomicInteger(0);
        List<Tuple2<String, Integer>> inputList = Arrays.stream("abcaba".split(""))
                .map(string -> new Tuple2<>(string, counter.getAndIncrement()))
                .collect(Collectors.toList());

        final JavaSparkContext sc = this.getSC();
        final JavaRDD<Tuple2<String, Integer>> inputRdd = sc.parallelize(inputList);

        // Build the reduce operator.
        SparkMaterializedGroupByOperator<Tuple2<String, Integer>, String> collocateByOperator =
                new SparkMaterializedGroupByOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                "field0")
                );

        // Set up the ChannelExecutors.
        final ChannelExecutor[] inputs = new ChannelExecutor[]{
                new TestChannelExecutor(inputRdd)
        };
        final ChannelExecutor[] outputs = new ChannelExecutor[]{
                new TestChannelExecutor()
        };

        // Execute.
        collocateByOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Iterable<Tuple2<String, Integer>>> originalResult =
                outputs[0].<Iterable<Tuple2<String, Integer>>>provideRdd().collect();
        Set<List<Tuple2<String, Integer>>> result = originalResult.stream()
                .map(this::toList)
                .collect(Collectors.toSet());

        final List[] expectedResults = {
                Arrays.asList(new Tuple2<>("a", 0), new Tuple2<>("a", 3), new Tuple2<>("a", 5)),
                Arrays.asList(new Tuple2<>("b", 1), new Tuple2<>("b", 4)),
                Arrays.asList(new Tuple2<>("c", 2))
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
        Assert.assertEquals(expectedResults.length, result.size());

    }

    private <T> List<T> toList(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }
}
