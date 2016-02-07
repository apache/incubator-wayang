package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by basti on 01/11/16.
 */
public class SparkCollectionSourceTest {

    @Test
    public void testExecution() {
//        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
//        JavaCollectionSource collectionSource = new JavaCollectionSource(
//                inputValues,
//                DataSetType.createDefault(Integer.class));
//        final Stream[] outputStreams = collectionSource.evaluate(new Stream[0], new FunctionCompiler());
//
//        Assert.assertEquals(1, outputStreams.length);
//        Stream<?> outputStream = outputStreams[0];
//        final Set<Object> outputValues = outputStream.collect(Collectors.toSet());
//        Assert.assertEquals(outputValues, inputValues);
    }
}
