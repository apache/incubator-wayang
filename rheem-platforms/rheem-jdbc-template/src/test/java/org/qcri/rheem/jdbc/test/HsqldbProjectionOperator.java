package org.qcri.rheem.jdbc.test;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;

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
