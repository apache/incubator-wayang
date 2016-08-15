package org.qcri.rheem.basic.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.function.ProjectionDescriptor;
import org.qcri.rheem.basic.operators.GroupByOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.ReduceOperator;
import org.qcri.rheem.basic.operators.test.TestSink;
import org.qcri.rheem.basic.operators.test.TestSource;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

/**
 * Test suite for the {@link ReduceByMapping}.
 */
public class ReduceByMappingTest {

    @Test
    public void testMapping() {
        // Construct a plan: source -> groupBy -> reduce -> sink.
        UnarySource<Tuple2<String, Integer>> source = new TestSource<>(DataSetType.createDefault(Tuple2.class));

        final ProjectionDescriptor<Tuple2<String, Integer>, String> keyDescriptor = new ProjectionDescriptor<>(
                DataUnitType.createBasicUnchecked(Tuple2.class),
                DataUnitType.createBasic(String.class),
                "field0");
        GroupByOperator<Tuple2<String, Integer>, String> groupBy = new GroupByOperator<>(
                keyDescriptor,
                DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createGroupedUnchecked(Tuple2.class)
        );
        source.connectTo(0, groupBy, 0);

        final ReduceDescriptor<Tuple2<String, Integer>> reduceDescriptor = new ReduceDescriptor<>(
                (a, b) -> a, DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        );
        ReduceOperator<Tuple2<String, Integer>> reduce = ReduceOperator.createGroupedReduce(
                reduceDescriptor,
                DataSetType.createGroupedUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        groupBy.connectTo(0, reduce, 0);

        UnarySink<Tuple2<String, Integer>> sink = new TestSink<>(DataSetType.createDefaultUnchecked(Tuple2.class));
        reduce.connectTo(0, sink, 0);
        RheemPlan plan = new RheemPlan();
        plan.addSink(sink);

        // Apply our mapping.
        Mapping mapping = new ReduceByMapping();
        for (PlanTransformation planTransformation : mapping.getTransformations()) {
            planTransformation.thatReplaces().transform(plan, Operator.FIRST_EPOCH + 1);
        }

        // Check that now we have this plan: source -> reduceBy -> sink.
        final Operator finalSink = plan.getSinks().iterator().next();
        final Operator inputOperator = finalSink.getEffectiveOccupant(0).getOwner();
        Assert.assertTrue(inputOperator instanceof ReduceByOperator);
        ReduceByOperator reduceBy = (ReduceByOperator) inputOperator;
        Assert.assertEquals(keyDescriptor, reduceBy.getKeyDescriptor());
        Assert.assertEquals(reduceDescriptor, reduceBy.getReduceDescriptor());
        Assert.assertEquals(source, reduceBy.getEffectiveOccupant(0).getOwner());
    }
}
