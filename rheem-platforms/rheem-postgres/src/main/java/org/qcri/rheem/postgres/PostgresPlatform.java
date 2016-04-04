package org.qcri.rheem.postgres;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Properties;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.platform.ChannelManager;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.channels.PostgresChannelManager;
import org.qcri.rheem.postgres.execution.PostgresExecutor;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Created by yidris on 3/22/16.
 */
public class PostgresPlatform extends Platform {

    private static final String PLATFORM_NAME = "postgres";

    private final Collection<Mapping> mappings = new LinkedList<>();

    private static PostgresPlatform instance = null;

    public Connection getConnection() {
        return connection;
    }

    private Connection connection = null;


    public static PostgresPlatform getInstance() {
        if (instance == null) {
            instance = new PostgresPlatform();
        }
        return instance;
    }

    private PostgresPlatform() {
        super(PLATFORM_NAME);
        this.initializeMappings();
        Properties default_properties = new Properties();
        default_properties.setProperty("postgres.conn_str", "jdbc:postgresql://localhost:5432/rheemdb");
        default_properties.setProperty("postgres.user", "rheem");
        default_properties.setProperty("postgres.pwd", "123");
        Properties properties = new Properties(default_properties);

        try {
            properties.load(new FileReader(new File("app.properties")));
        } catch (IOException e) {
            System.out.println("Could not find app.properties file, using default local postgres configuration.");
        }

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager
                    .getConnection(properties.getProperty("postgres.conn_str"),
                            properties.getProperty("postgres.user"),
                            properties.getProperty("postgres.pwd"));
            //connection.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }


    }

    private void initializeMappings() {
    }

    @Override
    public Collection<Mapping> getMappings() {
        return this.mappings;
    }

    @Override
    public boolean isExecutable() {
        return true;
    }

    @Override
    public Executor.Factory getExecutorFactory() {
        return () -> new PostgresExecutor(this);
    }

    @Override
    protected ChannelManager createChannelManager() {
        return new PostgresChannelManager(this);
    }

    @Override
    public PostgresChannelManager getChannelManager() {
        return (PostgresChannelManager) super.getChannelManager();
    }
}