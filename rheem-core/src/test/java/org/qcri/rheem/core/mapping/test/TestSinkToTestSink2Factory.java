package org.qcri.rheem.core.mapping.test;

import org.qcri.rheem.core.mapping.OperatorMatch;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanMatch;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.test.TestSink;
import org.qcri.rheem.core.plan.rheemplan.test.TestSink2;

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
