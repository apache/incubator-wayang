package org.apache.incubator.wayang.jdbc.test;

import org.apache.incubator.wayang.core.function.PredicateDescriptor;
import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.jdbc.operators.JdbcFilterOperator;

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
