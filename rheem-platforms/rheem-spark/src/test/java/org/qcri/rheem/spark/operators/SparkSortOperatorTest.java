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
 * Test suite for {@link SparkSortOperator}.
 */
public class SparkSortOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream = this.getSC().parallelize(Arrays.asList(6, 0, 1, 1, 5, 2));

        // Build the sort operator.
        SparkSortOperator<Integer> sortOperator =
                new SparkSortOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class)
                );

        // Execute the sort operator.
        final JavaRDDLike[] outputStreams = sortOperator.evaluate(new JavaRDDLike[]{inputStream}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((JavaRDD<Integer>) outputStreams[0]).collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(Arrays.asList(0, 1, 1, 2, 5, 6), result);

    }

}
