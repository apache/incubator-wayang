package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test suite for {@link SparkReduceByOperator}.
 */
public class SparkReduceByOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        List<Tuple2<String, Integer>> inputList = Arrays.stream("aaabbccccdeefff".split(""))
                .map(string -> new Tuple2<>(string, 1))
                .collect(Collectors.toList());
        RddChannel.Instance input = this.createRddChannelInstance(inputList);
        RddChannel.Instance output = this.createRddChannelInstance();


        // Build the reduce operator.
        SparkReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator =
                new SparkReduceByOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(String.class),
                                "field0"),
                        new ReduceDescriptor<>(
                                (a, b) -> {
                                    a.field1 += b.field1;
                                    return a;
                                }, DataUnitType.createGroupedUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(Tuple2.class)
                        ));

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(reduceByOperator, inputs, outputs);

        // Verify the outcome.
        final Iterable<Tuple2<String, Integer>> result = output.<Tuple2<String, Integer>>provideRdd().collect();
        final Set<Tuple2<String, Integer>> resultSet = new HashSet<>();
        result.forEach(resultSet::add);
        final Tuple2[] expectedResults = {
                new Tuple2<>("a", 3),
                new Tuple2<>("b", 2),
                new Tuple2<>("c", 4),
                new Tuple2<>("d", 1),
                new Tuple2<>("e", 2),
                new Tuple2<>("f", 3)
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, resultSet.contains(expected)));
        Assert.assertEquals(expectedResults.length, resultSet.size());

    }
}
