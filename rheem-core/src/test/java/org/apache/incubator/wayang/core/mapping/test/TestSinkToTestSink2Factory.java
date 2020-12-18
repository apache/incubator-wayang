package org.apache.incubator.wayang.core.mapping.test;

import org.apache.incubator.wayang.core.mapping.OperatorMatch;
import org.apache.incubator.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.incubator.wayang.core.mapping.SubplanMatch;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink;
import org.apache.incubator.wayang.core.plan.wayangplan.test.TestSink2;

/**
 * This factory replaces a {@link TestSink} by a
 * {@link TestSink2}.
 */
public class TestSinkToTestSink2Factory extends ReplacementSubplanFactory {

    @Override
    protected Operator translate(SubplanMatch subplanMatch, int epoch) {
        // Retrieve the matched TestSink.
        final OperatorMatch sinkMatch = subplanMatch.getOperatorMatches().get("sink");
        final TestSink testSink = (TestSink) sinkMatch.getOperator();

        // Translate the TestSink to a TestSink2.
        return new TestSink2<>(testSink.getInput().getType()).at(epoch);
    }

}
