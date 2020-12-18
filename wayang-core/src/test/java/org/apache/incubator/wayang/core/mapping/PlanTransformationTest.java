package org.apache.incubator.wayang.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.apache.incubator.wayang.core.mapping.test.TestSinkToTestSink2Factory;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.incubator.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySink;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySource;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink2;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.incubator.wayang.core.test.TestDataUnit;
import org.apache.incubator.wayang.core.types.DataSetType;

/**
 * Test suite for the {@link org.apache.incubator.wayang.core.mapping.PlanTransformation} class.
 */
public class PlanTransformationTest {

    @Test
    public void testReplace() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory).thatReplaces();

        // Apply the replacement strategy to the graph.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        Assert.assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        Assert.assertTrue(replacedSink instanceof TestSink2);
        Assert.assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

    @Test
    public void testIntroduceAlternative() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory);

        // Apply the replacement strategy to the graph.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        Assert.assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        Assert.assertTrue(replacedSink instanceof OperatorAlternative);
        OperatorAlternative operatorAlternative = (OperatorAlternative) replacedSink;
        Assert.assertEquals(2, operatorAlternative.getAlternatives().size());
        Assert.assertTrue(operatorAlternative.getAlternatives().get(0).getContainedOperator() instanceof TestSink);
        Assert.assertTrue(operatorAlternative.getAlternatives().get(1).getContainedOperator() instanceof TestSink2);
        Assert.assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

    @Test
    public void testFlatAlternatives() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Build the replacement strategy.
        ReplacementSubplanFactory replacementSubplanFactory = new TestSinkToTestSink2Factory();
        PlanTransformation planTransformation = new PlanTransformation(subplanPattern, replacementSubplanFactory);

        // Apply the replacement strategy to the graph twice.
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);
        planTransformation.transform(plan, Operator.FIRST_EPOCH + 1);

        // Check the correctness of the transformation.
        Assert.assertEquals(1, plan.getSinks().size());
        final Operator replacedSink = plan.getSinks().iterator().next();
        Assert.assertTrue(replacedSink instanceof OperatorAlternative);
        OperatorAlternative operatorAlternative = (OperatorAlternative) replacedSink;
        Assert.assertEquals(3, operatorAlternative.getAlternatives().size());
        Assert.assertTrue(operatorAlternative.getAlternatives().get(0).getContainedOperator() instanceof TestSink);
        Assert.assertTrue(operatorAlternative.getAlternatives().get(1).getContainedOperator() instanceof TestSink2);
        Assert.assertTrue(operatorAlternative.getAlternatives().get(2).getContainedOperator() instanceof TestSink2);
        Assert.assertEquals(source, replacedSink.getEffectiveOccupant(0).getOwner());
    }

}
