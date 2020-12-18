package org.apache.incubator.wayang.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySink;
import org.apache.incubator.wayang.core.plan.wayangplan.UnarySource;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSource;
import org.apache.incubator.wayang.core.test.TestDataUnit;
import org.apache.incubator.wayang.core.types.DataSetType;

import java.util.List;

/**
 * Test suite for the {@link SubplanPattern}.
 */
public class SubplanPatternTest {

    @Test
    public void testMatchSinkPattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

    @Test
    public void testMatchSourcePattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSetType.createDefault(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sourcePattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
    }

    @Test
    public void testMatchChainedPattern() {
        // Build the plan.
        UnarySource source = new TestSource(DataSetType.createDefault(TestDataUnit.class));
        UnarySink sink = new TestSink(DataSetType.createDefault(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        WayangPlan plan = new WayangPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSetType.createDefault(TestDataUnit.class)), false);
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSetType.createDefault(TestDataUnit.class)), false);
        sourcePattern.connectTo(0, sinkPattern, 0);
        SubplanPattern subplanPattern = SubplanPattern.fromOperatorPatterns(sourcePattern, sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan, Operator.FIRST_EPOCH);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

}
