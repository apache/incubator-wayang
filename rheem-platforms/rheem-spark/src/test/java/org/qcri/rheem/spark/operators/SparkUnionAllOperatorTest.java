package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkUnionAllOperator}.
 */
public class SparkUnionAllOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream0 = this.getSC().parallelize(Arrays.asList(6, 0, 1, 1, 5, 2));
        JavaRDD<Integer> inputStream1 = this.getSC().parallelize(Arrays.asList(1, 1, 9));

        // Build the UnionAll operator.
        SparkUnionAllOperator<Integer> unionAllOperator =
                new SparkUnionAllOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute the sort operator.
        final JavaRDDLike[] outputStreams = unionAllOperator.evaluate(new JavaRDD[]{inputStream0, inputStream1},
                new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((JavaRDD<Integer>) outputStreams[0]).collect();
        Assert.assertEquals(9, result.size());
        Assert.assertEquals(Arrays.asList(6, 0, 1, 1, 5, 2, 1, 1, 9), result);

    }

}
