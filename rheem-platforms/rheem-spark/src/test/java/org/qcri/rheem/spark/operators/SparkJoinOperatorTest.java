package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkJoinOperator}.
 */
public class SparkJoinOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        JavaRDD<Tuple2> inputStream0 = this.getSC().parallelize(Arrays.asList(new Tuple2(1,"b"), new Tuple2(1,"c"), new Tuple2(2,"d"),
                new Tuple2(3,"e")));
        JavaRDD<Tuple2> inputStream1 = this.getSC().parallelize(Arrays.asList(new Tuple2("x", 1), new Tuple2("y", 1), new Tuple2("z", 2),
                new Tuple2("w", 4)));
        // Build the Cartesian operator.
        SparkJoinOperator<Tuple2, Tuple2, Integer> join =
                new SparkJoinOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(String.class),
                                "field1"));

        // Execute the sort operator.
        final JavaRDDLike[] outputStreams = join.evaluate(new JavaRDD[]{inputStream0, inputStream1},
                new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputStreams.length);
        final List<Tuple2> result =
                ((JavaRDD<Tuple2>) outputStreams[0]).collect();
        Assert.assertEquals(5, result.size());
        Assert.assertEquals(result.get(0), new Tuple2(new Tuple2(1,"b"), new Tuple2("x", 1)));
        Assert.assertEquals(result.get(1), new Tuple2(new Tuple2(1,"b"), new Tuple2("y", 1)));
        Assert.assertEquals(result.get(2), new Tuple2(new Tuple2(1,"c"), new Tuple2("x", 1)));
        Assert.assertEquals(result.get(3), new Tuple2(new Tuple2(1,"c"), new Tuple2("y", 1)));
        Assert.assertEquals(result.get(4), new Tuple2(new Tuple2(2,"d"), new Tuple2("z", 2)));


    }

}
