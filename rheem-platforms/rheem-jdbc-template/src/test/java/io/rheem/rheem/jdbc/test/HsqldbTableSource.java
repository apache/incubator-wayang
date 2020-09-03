package io.rheem.rheem.jdbc.test;

import io.rheem.rheem.basic.operators.TableSource;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.jdbc.operators.JdbcFilterOperator;
import io.rheem.rheem.jdbc.operators.JdbcTableSource;

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
