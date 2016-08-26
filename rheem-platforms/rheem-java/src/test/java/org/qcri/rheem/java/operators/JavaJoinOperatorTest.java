package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaJoinOperator}.
 */
public class JavaJoinOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Stream<Tuple2<Integer, String>> inputStream0 = Arrays.asList(
                new Tuple2<>(1, "b"), new Tuple2<>(1, "c"), new Tuple2<>(2, "d"), new Tuple2<>(3, "e")
        ).stream();
        Stream<Tuple2<String, Integer>> inputStream1 = Arrays.asList(
                new Tuple2<>("x", 1), new Tuple2<>("y", 1), new Tuple2<>("z", 2), new Tuple2<>("w", 4)
        ).stream();

        // Build the Cartesian operator.
        JavaJoinOperator<Tuple2<Integer, String>, Tuple2<String, Integer>, Integer> join =
                new JavaJoinOperator<>(
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
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                createStreamChannelInstance(inputStream0),
                createStreamChannelInstance(inputStream1)
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
        evaluate(join, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> result =
                outputs[0].<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>>provideStream()
                        .collect(Collectors.toList());
        Collections.sort(result, (joinTuple1, joinTuple2) -> {
            int cmp = joinTuple1.getField0().getField0().compareTo(joinTuple2.getField0().getField0());
            if (cmp == 0) {
                cmp = joinTuple1.getField0().getField1().compareTo(joinTuple2.getField0().getField1());
            }
            if (cmp == 0) {
                cmp = joinTuple1.getField1().getField0().compareTo(joinTuple2.getField1().getField0());
            }
            if (cmp == 0) {
                cmp = joinTuple1.getField1().getField1().compareTo(joinTuple2.getField1().getField1());
            }
            return cmp;
        });
        final List<Tuple2<Tuple2<Integer, String>, Tuple2<String, Integer>>> expectedResult = Arrays.asList(
                new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("x", 1)),
                new Tuple2<>(new Tuple2<>(1, "b"), new Tuple2<>("y", 1)),
                new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("x", 1)),
                new Tuple2<>(new Tuple2<>(1, "c"), new Tuple2<>("y", 1)),
                new Tuple2<>(new Tuple2<>(2, "d"), new Tuple2<>("z", 2))
        );

        Assert.assertEquals(expectedResult, result);


    }

}
