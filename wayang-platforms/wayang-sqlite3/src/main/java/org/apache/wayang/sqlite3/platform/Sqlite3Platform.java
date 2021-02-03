package org.apache.wayang.sqlite3.platform;

import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class Sqlite3Platform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "SQLite3";

    private static final String CONFIG_NAME = "sqlite3";

    private static Sqlite3Platform instance = null;

    public static Sqlite3Platform getInstance() {
        if (instance == null) {
            instance = new Sqlite3Platform();
        }
        return instance;
    }

    protected Sqlite3Platform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.sqlite.JDBC.class.getName();
    }

}
