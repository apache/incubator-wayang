package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.spark.channels.RddChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Test suite for {@link SparkJoinOperator}.
 */
public class SparkCoGroupOperatorTest extends SparkOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        RddChannel.Instance input0 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")));
        RddChannel.Instance input1 = this.createRddChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the operator.
        SparkCoGroupOperator<Tuple2, Tuple2, Integer> coGroup =
                new SparkCoGroupOperator<>(
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class)
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input0, input1};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        this.evaluate(coGroup, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>> result =
                output.<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>>provideRdd().collect();
        Collection<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> expectedGroups =
                new ArrayList<>(Arrays.asList(
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Arrays.asList(new Tuple2<>(1, "b"), new Tuple2<>(1, "c")),
                                Arrays.asList(new Tuple2<>("x", 1), new Tuple2<>("y", 1))
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(2, "d")),
                                Collections.singletonList(new Tuple2<>("z", 2))
                        ), new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.singletonList(new Tuple2<>(3, "e")),
                                Collections.emptyList()
                        ),
                        new Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>(
                                Collections.emptyList(),
                                Collections.singletonList(new Tuple2<>("w", 4))
                        )
                ));

        ResultLoop:
        for (Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> resultCoGroup : result) {
            for (Iterator<Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>>> i = expectedGroups.iterator();
                 i.hasNext(); ) {
                Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expectedGroup = i.next();
                if (this.compare(expectedGroup, resultCoGroup)) {
                    i.remove();
                    continue ResultLoop;
                }
            }
            Assert.fail(String.format("Unexpected group: %s", resultCoGroup));
        }
        Assert.assertTrue(
                String.format("Missing groups: %s", expectedGroups),
                expectedGroups.isEmpty()
        );
    }

    private boolean compare(Tuple<Collection<Tuple2<Integer, String>>, Collection<Tuple2<String, Integer>>> expected,
                            Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>> actual) {
        return this.compareGroup(expected.field0, actual.field0) && this.compareGroup(expected.field1, actual.field1);
    }

    private <T> boolean compareGroup(Collection<T> expected, Iterable<T> actual) {
        if (expected == null) return actual == null;
        if (actual == null) return false;

        return RheemCollections.asSet(expected).equals(RheemCollections.asSet(actual));
    }

}
