package org.qcri.rheem.jdbc.test;

import org.qcri.rheem.jdbc.JdbcPlatformTemplate;

/**
 * {@link JdbcPlatformTemplate} implementation based on HSQLDB for test purposes.
 */
public class HsqldbPlatform extends JdbcPlatformTemplate {

    private static final HsqldbPlatform instance = new HsqldbPlatform();

    public HsqldbPlatform() {
        super("HSQLDB (test)");
    }

    @Override
    protected void initializeMappings() {
        // TODO
    }

    public static HsqldbPlatform getInstance() {
        return instance;
    }

    @Override
    protected String getPlatformId() {
        return "hsqldb";
    }

    @Override
    protected String getJdbcDriverClassName() {
        return org.hsqldb.jdbcDriver.class.getName();
    }
}
