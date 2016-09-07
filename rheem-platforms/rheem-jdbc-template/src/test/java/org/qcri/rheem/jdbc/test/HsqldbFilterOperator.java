package org.qcri.rheem.jdbc.test;

import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;

import java.util.List;

/**
 * Test implementation of {@link JdbcFilterOperator}.
 */
public class HsqldbFilterOperator extends JdbcFilterOperator {

    public HsqldbFilterOperator(PredicateDescriptor predicateDescriptor) {
        super(predicateDescriptor);
    }

    @Override
    public HsqldbPlatform getPlatform() {
        return HsqldbPlatform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        throw new UnsupportedOperationException();
    }
}
