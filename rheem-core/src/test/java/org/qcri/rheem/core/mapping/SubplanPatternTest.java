package org.qcri.rheem.core.mapping;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.plan.Sink;
import org.qcri.rheem.core.plan.Source;
import org.qcri.rheem.core.plan.test.TestSink;
import org.qcri.rheem.core.plan.test.TestSource;
import org.qcri.rheem.core.test.TestDataUnit;
import org.qcri.rheem.core.types.DataSet;

import java.util.List;

/**
 * Test suite for the {@link SubplanPattern}.
 */
public class SubplanPatternTest {

    @Test
    public void testMatchSinkPattern() {
        // Build the plan.
        Source source = new TestSource(DataSet.flatAndBasic(TestDataUnit.class));
        Sink sink = new TestSink(DataSet.flatAndBasic(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSet.flatAndBasic(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

    @Test
    public void testMatchSourcePattern() {
        // Build the plan.
        Source source = new TestSource(DataSet.flatAndBasic(TestDataUnit.class));
        Sink sink = new TestSink(DataSet.flatAndBasic(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSet.flatAndBasic(TestDataUnit.class)), false);
        SubplanPattern subplanPattern = SubplanPattern.createSingleton(sourcePattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
    }

    @Test
    public void testMatchChainedPattern() {
        // Build the plan.
        Source source = new TestSource(DataSet.flatAndBasic(TestDataUnit.class));
        Sink sink = new TestSink(DataSet.flatAndBasic(TestDataUnit.class));
        source.connectTo(0, sink, 0);
        PhysicalPlan plan = new PhysicalPlan();
        plan.addSink(sink);

        // Build the pattern.
        OperatorPattern sourcePattern = new OperatorPattern("source", new TestSource(DataSet.flatAndBasic(TestDataUnit.class)), false);
        OperatorPattern sinkPattern = new OperatorPattern("sink", new TestSink(DataSet.flatAndBasic(TestDataUnit.class)), false);
        sourcePattern.connectTo(0, sinkPattern, 0);
        SubplanPattern subplanPattern = SubplanPattern.fromOperatorPatterns(sourcePattern, sinkPattern);

        // Match the pattern against the plan.
        final List<SubplanMatch> matches = subplanPattern.match(plan);

        // Evaluate the matches.
        Assert.assertEquals(1, matches.size());
        final SubplanMatch match = matches.get(0);
        Assert.assertEquals(source, match.getOperatorMatches().get("source").getOperator());
        Assert.assertEquals(sink, match.getOperatorMatches().get("sink").getOperator());
    }

}
