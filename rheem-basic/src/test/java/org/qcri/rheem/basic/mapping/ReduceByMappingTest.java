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
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.plan.UnarySink;
import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSet;
import org.qcri.rheem.core.types.DataUnitGroupType;
import org.qcri.rheem.core.types.GroupedDataSet;

/**
 * Test suite for the {@link ReduceByMapping}.
 */
public class ReduceByMappingTest {

    @Test
    public void testMapping() {
        // Construct a plan: source -> groupBy -> reduce -> sink.
        UnarySource source = new TestSource<>(DataSet.flatAndBasic(Tuple2.class));

        final ProjectionDescriptor keyDescriptor = new ProjectionDescriptor(
                new BasicDataUnitType(Tuple2.class), new BasicDataUnitType(String.class), "field0");
        GroupByOperator groupBy = new GroupByOperator(
                keyDescriptor,
                DataSet.flatAndBasic(Tuple2.class),
                new GroupedDataSet(new BasicDataUnitType(Tuple2.class))
        );
        source.connectTo(0, groupBy, 0);

        final ReduceDescriptor reduceDescriptor = new ReduceDescriptor(new DataUnitGroupType(new BasicDataUnitType(Tuple2.class)),
                new BasicDataUnitType(Tuple2.class),
                (a, b) -> a);
        ReduceOperator reduce = new ReduceOperator(
                reduceDescriptor,
                new GroupedDataSet(new BasicDataUnitType(Tuple2.class)),
                DataSet.flatAndBasic(Tuple2.class)
        );
        groupBy.connectTo(0, reduce, 0);

        UnarySink sink = new TestSink<>(DataSet.flatAndBasic(Tuple2.class));
        reduce.connectTo(0, sink, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.addSink(sink);

        // Apply our mapping.
        Mapping mapping = new ReduceByMapping();
        for (PlanTransformation planTransformation : mapping.getTransformations()) {
            planTransformation.transform(plan);
        }

        // Check that now we have this plan: source -> reduceBy -> sink.
        final Operator finalSink = plan.getSinks().iterator().next();
        final Operator inputOperator = finalSink.getInputOperator(0);
        Assert.assertTrue(inputOperator instanceof ReduceByOperator);
        ReduceByOperator reduceBy = (ReduceByOperator) inputOperator;
        Assert.assertEquals(keyDescriptor, reduceBy.getKeyDescriptor());
        Assert.assertEquals(reduceDescriptor, reduceBy.getReduceDescriptor());
        Assert.assertEquals(source, reduceBy.getInputOperator(0));
    }
}
