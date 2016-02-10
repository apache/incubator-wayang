package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDDLike;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Test suite for the {@link SparkCollectionSource}.
 */
public class SparkCollectionSourceTest {

    @Test
    public void testExecution() {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        SparkCollectionSource<Integer> collectionSource = new SparkCollectionSource<>(
                inputValues,
                DataSetType.createDefault(Integer.class));
        final JavaRDDLike[] outputStreams = collectionSource.evaluate(new JavaRDDLike[0], new FunctionCompiler());

        Assert.assertEquals(1, outputStreams.length);
        JavaRDDLike outputStream = outputStreams[0];
        final Set<Integer> outputValues = new HashSet<>((List<Integer>) outputStream.collect());
        Assert.assertEquals(outputValues, inputValues);
    }
}
