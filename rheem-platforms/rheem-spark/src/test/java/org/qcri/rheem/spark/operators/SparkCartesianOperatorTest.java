package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;

//TODO Fix this!
//import org.qcri.rheem.basic.data.Tuple2;
import scala.Tuple2;

import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkCartesianOperator}.
 */
public class SparkCartesianOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Integer> inputStream0 = this.getSC().parallelize(Arrays.asList(1, 2));
        JavaRDD<String> inputStream1 = this.getSC().parallelize(Arrays.asList("a", "b", "c"));

        // Build the Cartesian operator.
        SparkCartesianOperator<Integer, String> cartesianOperator =
                new SparkCartesianOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createDefaultUnchecked(String.class));

        // Execute the sort operator.
        final JavaRDDLike[] outputStreams = cartesianOperator.evaluate(new JavaRDD[]{inputStream0, inputStream1},
                new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Tuple2<Integer, String>> result =
                ((JavaPairRDD<Integer, String>) outputStreams[0]).collect();
        Assert.assertEquals(6, result.size());
        Assert.assertEquals(result.get(0), new Tuple2(1,"a"));

    }

}
