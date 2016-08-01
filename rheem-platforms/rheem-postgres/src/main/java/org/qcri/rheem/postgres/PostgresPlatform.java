package org.qcri.rheem.postgres;

import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.jdbc.JdbcPlatformTemplate;
import org.qcri.rheem.postgres.mapping.PostgresFilterMapping;
import org.qcri.rheem.postgres.mapping.PostgresProjectionMapping;

/**
 * {@link Platform} implementation for SQLite3.
 */
public class PostgresPlatform extends JdbcPlatformTemplate {

    private static final String PLATFORM_NAME = "PostgreSQL";

    private static PostgresPlatform instance = null;

    public static PostgresPlatform getInstance() {
        if (instance == null) {
            instance = new PostgresPlatform();
        }
        return instance;
    }

    protected PostgresPlatform() {
        super(PLATFORM_NAME);
    }

    @Override
    protected void initializeMappings() {
        this.mappings.add(new PostgresFilterMapping());
        this.mappings.add(new PostgresProjectionMapping());
    }

    @Override
    public String getPlatformId() {
        return "postgres";
    }

    @Override
    public String getJdbcDriverClassName() {
        return org.postgresql.Driver.class.getName();
    }

}