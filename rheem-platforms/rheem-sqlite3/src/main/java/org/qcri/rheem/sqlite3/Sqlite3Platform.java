package org.qcri.rheem.sqlite3;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class Sqlite3Platform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "SQLite3";

    protected Sqlite3Platform() {
        super(PLATFORM_NAME);
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
