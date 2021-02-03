package org.apache.wayang.sqlite3.operators;

import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.jdbc.operators.JdbcTableSource;
import org.apache.wayang.sqlite3.platform.Sqlite3Platform;

import java.util.List;

/**
 * Implementation of the {@link TableSource} for the {@link Sqlite3Platform}.
 */
public class Sqlite3TableSource extends JdbcTableSource {

    /**
     * Creates a new instance.
     *
     * @see TableSource#TableSource(String, String...)
     */
    public Sqlite3TableSource(String tableName, String... columnNames) {
        super(tableName, columnNames);
    }

    public Sqlite3TableSource(Sqlite3TableSource that) {
        super(that);
    }

    @Override
    public Sqlite3Platform getPlatform() {
        return Sqlite3Platform.getInstance();
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException("Input channels are not supported.");
    }

}
