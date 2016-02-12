package org.qcri.rheem.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plan.rheemplan.UnarySink;
import org.qcri.rheem.core.plan.rheemplan.UnarySource;
import org.qcri.rheem.core.plan.rheemplan.test.TestSink;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;
import org.qcri.rheem.core.test.TestDataUnit;
import org.qcri.rheem.core.types.DataSetType;

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
        RheemPlan plan = new RheemPlan();
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
        RheemPlan plan = new RheemPlan();
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
        RheemPlan plan = new RheemPlan();
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
