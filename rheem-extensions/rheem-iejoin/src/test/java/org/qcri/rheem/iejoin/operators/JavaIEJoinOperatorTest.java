package org.qcri.rheem.iejoin.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test suite for {@link JavaIEJoinOperator}.
 */
public class JavaIEJoinOperatorTest extends JavaExecutionOperatorTestBase {


    @Test
    public void testExecution() {
        Record r1 = new Record(100, 10);
        Record r2 = new Record(200, 20);
        Record r3 = new Record(300, 30);
        Record r11 = new Record(250, 5);
        // Prepare test data.
        Stream<Record> inputStream0 = Arrays.asList(r1, r2, r3, r11).stream();
        Stream<Record> inputStream1 = Arrays.asList(r1, r2, r3).stream();

        // Build the Cartesian operator.
        JavaIEJoinOperator<Integer, Integer, Record> IEJoinOperator =
                new JavaIEJoinOperator<Integer, Integer, Record>(
                        DataSetType.createDefaultUnchecked(Record.class),
                        DataSetType.createDefaultUnchecked(Record.class),
                        //0, 0, JoinCondition.GreaterThan, 1, 1, JoinCondition.LessThan
                        new TransformationDescriptor<Record, Integer>(word -> (Integer) word.getField(0),
                                DataUnitType.<Record>createBasic(Record.class),
                                DataUnitType.<Integer>createBasicUnchecked(Integer.class)
                        ),
                        new TransformationDescriptor<Record, Integer>(word -> (Integer) word.getField(0),
                                DataUnitType.<Record>createBasic(Record.class),
                                DataUnitType.<Integer>createBasicUnchecked(Integer.class)
                        ),
                        IEJoinMasterOperator.JoinCondition.GreaterThan,
                        new TransformationDescriptor<Record, Integer>(word -> (Integer) word.getField(1),
                                DataUnitType.<Record>createBasic(Record.class),
                                DataUnitType.<Integer>createBasicUnchecked(Integer.class)
                        ),
                        new TransformationDescriptor<Record, Integer>(word -> (Integer) word.getField(1),
                                DataUnitType.<Record>createBasic(Record.class),
                                DataUnitType.<Integer>createBasicUnchecked(Integer.class)
                        ),
                        IEJoinMasterOperator.JoinCondition.LessThan
                );

        // Set up the ChannelInstances.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{
                JavaExecutionOperatorTestBase.createStreamChannelInstance(inputStream0),
                JavaExecutionOperatorTestBase.createStreamChannelInstance(inputStream1)
        };
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{JavaExecutionOperatorTestBase.createStreamChannelInstance()};
        JavaExecutionOperatorTestBase.evaluate(IEJoinOperator, inputs, outputs);

        // Verify the outcome.
        final List<Tuple2<Record, Record>> result = outputs[0].<Tuple2<Record, Record>>provideStream().collect(Collectors.toList());
        Assert.assertEquals(2, result.size());
        //Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
