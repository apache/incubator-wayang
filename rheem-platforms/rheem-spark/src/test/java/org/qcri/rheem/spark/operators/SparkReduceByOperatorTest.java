package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitGroupType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link SparkReduceByOperator}.
 */
public class SparkReduceByOperatorTest {

    @Test
    public void testExecution() {
//        // Prepare test data.
//        Stream<Tuple2<String, Integer>> inputStream = Arrays.stream("aaabbccccdeefff".split(""))
//                .map(string -> new Tuple2<>(string, 1));
//
//        // Build the reduce operator.
//        JavaReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator =
//                new JavaReduceByOperator<>(
//                        DataSetType.createDefaultUnchecked(Tuple2.class),
//                        new ProjectionDescriptor<>(
//                                DataUnitType.createBasicUnchecked(Tuple2.class),
//                                DataUnitType.createBasic(String.class),
//                                "field0"),
//                        new ReduceDescriptor<>(
//                                DataUnitType.createGroupedUnchecked(Tuple2.class),
//                                DataUnitType.createBasicUnchecked(Tuple2.class),
//                                (a, b) -> {
//                                    a.field1 += b.field1;
//                                    return a;
//                                }));
//
//        // Execute the reduce operator.
//        final Stream[] outputStreams = reduceByOperator.evaluate(new Stream[]{inputStream}, new FunctionCompiler());
//
//        // Verify the outcome.
//        Assert.assertEquals(1, outputStreams.length);
//        final Set<Tuple2<String, Integer>> result =
//                ((Stream<Tuple2<String, Integer>>) outputStreams[0]).collect(Collectors.toSet());
//        final Tuple2[] expectedResults = {
//                new Tuple2<>("a", 3),
//                new Tuple2<>("b", 2),
//                new Tuple2<>("c", 4),
//                new Tuple2<>("d", 1),
//                new Tuple2<>("e", 2),
//                new Tuple2<>("f", 3)
//        };
//        Arrays.stream(expectedResults)
//                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
//        Assert.assertEquals(expectedResults.length, result.size());

    }
}
