package org.qcri.rheem.sqlite3;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;
import org.qcri.rheem.jdbc.channels.SqlQueryChannel;
import org.qcri.rheem.sqlite3.mappings.FilterMapping;
import org.qcri.rheem.sqlite3.mappings.ProjectionMapping;
import org.qcri.rheem.sqlite3.mappings.TableSourceMapping;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class Sqlite3Platform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "SQLite3";

    private static Sqlite3Platform instance = null;

    /**
     * {@link ChannelDescriptor} for {@link SqlQueryChannel}s with this instance.
     */
    public final SqlQueryChannel.Descriptor sqlQueryChannelDescriptor = new SqlQueryChannel.Descriptor(this);

    protected Sqlite3Platform() {
        super(PLATFORM_NAME);
    }

    @Override
    protected void initializeMappings() {
        this.mappings.add(new TableSourceMapping());
        this.mappings.add(new FilterMapping());
        this.mappings.add(new ProjectionMapping());
    }

    public static Sqlite3Platform getInstance() {
        if (instance == null) {
            instance = new Sqlite3Platform();
        }
        return instance;
    }

    @Override
    protected String getPlatformId() {
        return "sqlite3";
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.sqlite.JDBC.class.getName();
    }
}
