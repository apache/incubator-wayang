package org.qcri.rheem.jdbc.execution;

import org.qcri.rheem.core.api.exception.RheemException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * This class describes a database.
 */
public class DatabaseDescriptor {

    private final String jdbcUrl, user, password, jdbcDriverClassName;

    /**
     * Creates a new instance.
     *
     * @param jdbcUrl             JDBC URL to the database
     * @param user                <i>optional</i> user name or {@code null}
     * @param password            <i>optional</i> password or {@code null}
     * @param jdbcDriverClassName name of the JDBC driver {@link Class} to access the database;
     *                            required for {@link #createJdbcConnection()}
     */
    public DatabaseDescriptor(String jdbcUrl, String user, String password, String jdbcDriverClassName) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.jdbcDriverClassName = jdbcDriverClassName;
    }

    /**
     * Creates a {@link Connection} to the database described by this instance.
     *
     * @return the {@link Connection}
     */
    public Connection createJdbcConnection() {
        try {
            Class.forName(this.jdbcDriverClassName);
        } catch (ClassNotFoundException e) {
            throw new RheemException("Could not load JDBC driver.", e);
        }

        try {
            return DriverManager.getConnection(this.jdbcUrl, this.user, this.password);
        } catch (SQLException e) {
            throw new RheemException("Could not connect to database.", e);
        }
    }
}
