package io.rheem.rheem.postgres.operators;

import io.rheem.rheem.basic.operators.TableSource;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.jdbc.operators.JdbcTableSource;

import java.util.List;

/**
 * PostgreSQL implementation for the {@link TableSource}.
 */
public class PostgresTableSource extends JdbcTableSource implements PostgresExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public PostgresTableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresTableSource(JdbcTableSource that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("This operator has no input channels.");
    }
}
