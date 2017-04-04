package org.qcri.rheem.postgres.platform;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.platform.JdbcPlatformTemplate;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class PostgresPlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "PostgreSQL";

    private static final String CONFIG_NAME = "postgres";

    private static PostgresPlatform instance = null;

    public static PostgresPlatform getInstance() {
        if (instance == null) {
            instance = new PostgresPlatform();
        }
        return instance;
    }

    protected PostgresPlatform() {
        super(PLATFORM_NAME, CONFIG_NAME);
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.postgresql.Driver.class.getName();
    }

}