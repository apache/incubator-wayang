package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

/**
 * Test suite for {@link SparkFilterOperator}.
 */
public class SparkFilterOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream = this.getSC().parallelize(Arrays.asList(0, 1, 1, 2, 6));

        // Build the distinct operator.
        SparkFilterOperator<Integer> filterOperator =
                new SparkFilterOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        (Predicate<Integer> & Serializable) item -> item > 0
                );

        // Execute the distinct operator.
        final JavaRDDLike[] outputStreams = filterOperator.evaluate(new JavaRDD[]{inputStream},
                new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((JavaRDD<Integer>) outputStreams[0]).collect();
        Assert.assertEquals(4, result.size());
        Assert.assertEquals(Arrays.asList(1, 1, 2, 6), result);

    }

}
