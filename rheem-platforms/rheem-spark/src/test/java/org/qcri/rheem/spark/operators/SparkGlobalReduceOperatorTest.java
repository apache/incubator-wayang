package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Test suite for {@link SparkGlobalReduceOperator}.
 */
public class SparkGlobalReduceOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        final JavaRDD<Integer> inputRdd = super.getSC().parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

        // Build the reduce operator.
        SparkGlobalReduceOperator<Integer> globalReduce =
                new SparkGlobalReduceOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ReduceDescriptor<>(
                                DataUnitType.createGrouped(Integer.class),
                                DataUnitType.createBasic(Integer.class),
                                (a, b) -> a + b
                        )
                );

        // Execute the reduce operator.
        final JavaRDDLike[] outputStreams = globalReduce.evaluate(new JavaRDDLike[]{inputRdd}, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result = outputStreams[0].collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Integer.valueOf((10 + 1) * (10 / 2)), result.get(0)); // Props to Gauss!

    }

    @Ignore("Spark cannot reduce empty collections.")
    @Test
    public void testExecutionWithoutData() {
        // Prepare test data.
        final JavaRDD<Integer> inputRdd = super.getSC().parallelize(Collections.emptyList());

        // Build the reduce operator.
        SparkGlobalReduceOperator<Integer> globalReduce =
                new SparkGlobalReduceOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ReduceDescriptor<>(
                                DataUnitType.createGrouped(Integer.class),
                                DataUnitType.createBasic(Integer.class),
                                (a, b) -> a + b
                        )
                );

        // Execute the reduce operator.
        final JavaRDDLike[] outputStreams = globalReduce.evaluate(new JavaRDDLike[]{inputRdd}, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result = outputStreams[0].collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Integer.valueOf((10 + 1) * (10 / 2)), result.get(0)); // Props to Gauss!

    }
}
