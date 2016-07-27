package org.qcri.rheem.jdbc.test;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;
import org.qcri.rheem.jdbc.operators.JdbcTableSource;

import java.util.List;

/**
 * Test implementation of {@link JdbcFilterOperator}.
 */
public class HsqldbTableSource extends JdbcTableSource {

    public HsqldbTableSource(String tableName) {
        super(tableName);
    }

    @Override
    public Platform getPlatform() {
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
