package org.qcri.rheem.spark.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.JoinCondition;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.compiler.FunctionCompiler;

import java.util.Arrays;
import java.util.List;

/**
 * Test suite for {@link SparkIEJoinOperator}.
 */
public class SparkIESelfJoinOperatorTest extends SparkOperatorTestBase {


    @Test
    public void testExecution() {
        //Record r1 = new Record(100, 10);
        Record r2 = new Record(200, 20);
        Record r3 = new Record(300, 30);
        Record r11 = new Record(250, 5);
        // Prepare test data.
        RddChannel.Instance input = this.createRddChannelInstance(Arrays.asList(r2, r3, r11));
        RddChannel.Instance output = this.createRddChannelInstance();

        // Build the Cartesian operator.
        SparkIESelfJoinOperator<Integer, Integer> IESelfJoinOperator =
                new SparkIESelfJoinOperator<Integer, Integer>(
                        DataSetType.createDefaultUnchecked(Record.class),
                        0, JoinCondition.GreaterThan, 1, JoinCondition.LessThan
                        /*new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field0"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1"),
                        new ProjectionDescriptor<Record, Integer>(
                                DataUnitType.createBasicUnchecked(Record.class),
                                DataUnitType.createBasic(Integer.class),
                                "field1")*/
                );

        // Set up the ChannelInstances.
        final ChannelInstance[] inputs = new ChannelInstance[]{input};
        final ChannelInstance[] outputs = new ChannelInstance[]{output};

        // Execute.
        IESelfJoinOperator.evaluate(inputs, outputs, new FunctionCompiler(), this.sparkExecutor);

        // Verify the outcome.
        final List<Tuple2<Record, Record>> result = output.<Tuple2<Record, Record>>provideRdd().collect();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(result.get(0), new Tuple2<Record, Record>(r11, r2));
        //Assert.assertEquals(result.get(0), new Tuple2(1, "a"));

    }

}
