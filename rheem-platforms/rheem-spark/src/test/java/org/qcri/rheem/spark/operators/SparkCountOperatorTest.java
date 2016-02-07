package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;
import scala.Int;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Test suite for {@link SparkCountOperator}.
 */
public class SparkCountOperatorTest extends SparkOperatorTestBase{

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream = this.getSC().parallelize(Arrays.asList(1, 2, 3, 4, 5));

        // Build the count operator.
        SparkCountOperator<Integer> countOperator =
                new SparkCountOperator<>(DataSetType.createDefaultUnchecked(Integer.class));

        // Execute the count operator.
        final JavaRDDLike[] outputStreams = countOperator.evaluate(new JavaRDD[]{inputStream}, new FunctionCompiler());


        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Integer> result =
                ((JavaRDD<Integer>) outputStreams[0]).collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(Long.valueOf(5), result.get(0));

    }

}
