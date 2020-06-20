package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Test suite for {@link JavaJoinOperator}.
 */
public class JavaCoGroupOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        CollectionChannel.Instance input0 = createCollectionChannelInstance(Arrays.asList(
                new Tuple2<>(1, "b"),
                new Tuple2<>(1, "c"),
                new Tuple2<>(2, "d"),
                new Tuple2<>(3, "e")
        ));
        CollectionChannel.Instance input1 = this.createCollectionChannelInstance(Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)));
        CollectionChannel.Instance output = createCollectionChannelInstance();

        // Build the Cartesian operator.
        JavaCoGroupOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> coGroup =
                new JavaCoGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        DataSetType.createDefaultUnchecked(Tuple2.class),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<>(
                                DataUnitType.createBasicUnchecked(Tuple2.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"));

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{input0, input1};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{output};
        evaluate(coGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Tuple2<Iterable<Tuple2<Integer, String>>, Iterable<Tuple2<String, Integer>>>> result =
                output.provideCollection();
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
