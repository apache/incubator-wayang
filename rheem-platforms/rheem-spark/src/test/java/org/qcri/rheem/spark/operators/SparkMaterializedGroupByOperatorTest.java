package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDDLike;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Test suite for {@link SparkMaterializedGroupByOperator}.
 */
public class SparkMaterializedGroupByOperatorTest extends SparkOperatorTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecution() {
        // Prepare test data.
        AtomicInteger counter = new AtomicInteger(0);
        List<Tuple2<String, Integer>> inputList = Arrays.stream("abcaba".split(""))
                .map(string -> new Tuple2<>(string, counter.getAndIncrement()))
                .collect(Collectors.toList());

        final JavaSparkContext sc = this.getSC();
        final JavaRDD<Tuple2<String, Integer>> inputRdd = sc.parallelize(inputList);

        // Build the reduce operator.
        SparkMaterializedGroupByOperator<Tuple2<String, Integer>, String> collocateByOperator =
                new SparkMaterializedGroupByOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                "field0")
                );

        // Execute the reduce operator.
        final JavaRDDLike[] outputRdds = collocateByOperator.evaluate(new JavaRDDLike[]{inputRdd}, new FunctionCompiler());

        // Verify the outcome.
        Assert.assertEquals(1, outputRdds.length);
        final List originalResult = outputRdds[0].collect();
        Set<List<Tuple2<String, Integer>>> result = new HashSet<>();
        for (Object resultIterable : originalResult) {
            List<Tuple2<String, Integer>> resultList = new LinkedList<>();
            for (Tuple2<String, Integer> resultElement : (Iterable<Tuple2<String, Integer>>) resultIterable) {
                resultList.add(resultElement);
            }
            result.add(resultList);
        }

        final List[] expectedResults = {
                Arrays.asList(new Tuple2<>("a", 0), new Tuple2<>("a", 3), new Tuple2<>("a", 5)),
                Arrays.asList(new Tuple2<>("b", 1), new Tuple2<>("b", 4)),
                Arrays.asList(new Tuple2<>("c", 2))
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
        Assert.assertEquals(expectedResults.length, result.size());

    }
}
