package org.qcri.rheem.postgres;


import org.qcri.rheem.postgres.platform.PostgresPlatform;
import org.qcri.rheem.postgres.plugin.PostgresPlugin;

/**
 * Register for relevant components of this module.
 */
public class Postgres {

    private final static PostgresPlugin PLUGIN = new PostgresPlugin();

    /**
     * Retrieve the {@link PostgresPlugin}.
     *
     * @return the {@link PostgresPlugin}
     */
    public static PostgresPlugin plugin() {
        return PLUGIN;
    }


    /**
     * Retrieve the {@link PostgresPlatform}.
     *
     * @return the {@link PostgresPlatform}
     */
    public static PostgresPlatform platform() {
        return PostgresPlatform.getInstance();
    }

}
