package org.apache.incubator.wayang.jdbc.test;

import org.apache.incubator.wayang.core.platform.ChannelDescriptor;
import org.apache.incubator.wayang.jdbc.operators.JdbcFilterOperator;
import org.apache.incubator.wayang.jdbc.operators.JdbcProjectionOperator;

import java.util.List;

/**
 * Test implementation of {@link JdbcFilterOperator}.
 */
public class HsqldbProjectionOperator extends JdbcProjectionOperator {

    public HsqldbProjectionOperator(String... names) {
        super(names);
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
