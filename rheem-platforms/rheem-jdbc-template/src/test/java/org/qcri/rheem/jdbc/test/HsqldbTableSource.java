package org.qcri.rheem.jdbc.test;

import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;
import org.qcri.rheem.jdbc.operators.JdbcTableSource;

import java.util.List;

/**
 * Test implementation of {@link JdbcFilterOperator}.
 */
public class HsqldbTableSource extends JdbcTableSource {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public HsqldbTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
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
