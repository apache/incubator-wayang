package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test suite for {@link SparkMaterializedGroupByOperator}.
 */
public class SparkMaterializedGroupByOperatorTest extends SparkOperatorTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecution() {
        // Prepare test data.
        AtomicInteger counter = new AtomicInteger(0);
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.stream("abcaba".split(""))
                .map(string -> new Tuple2<>(string, counter.getAndIncrement()))
                .collect(Collectors.toList()));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the reduce operator.
        SparkMaterializedGroupByOperator<Tuple2<String, Integer>, String> collocateByOperator =
                new SparkMaterializedGroupByOperator<>(
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                "field0"),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createGroupedUnchecked(Tuple2.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(collocateByOperator, inputs, outputs);

        // Verify the outcome.
        final List<Iterable<Tuple2<String, Integer>>> originalResult =
                output.<Iterable<Tuple2<String, Integer>>>provideRdd().collect();
        Set<List<Tuple2<String, Integer>>> result = originalResult.stream()
                .map(this::toList)
                .collect(Collectors.toSet());

        final List[] expectedResults = {
                Arrays.asList(new Tuple2<>("a", 0), new Tuple2<>("a", 3), new Tuple2<>("a", 5)),
                Arrays.asList(new Tuple2<>("b", 1), new Tuple2<>("b", 4)),
                Arrays.asList(new Tuple2<>("c", 2))
        };
        Arrays.stream(expectedResults)
                .forEach(expected -> Assert.assertTrue("Not contained: " + expected, result.contains(expected)));
        Assert.assertEquals(expectedResults.length, result.size());

    }

    private <T> List<T> toList(Iterable<T> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }
}
