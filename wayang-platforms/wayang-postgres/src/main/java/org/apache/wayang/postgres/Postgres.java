package org.apache.wayang.postgres;


import org.apache.wayang.postgres.platform.PostgresPlatform;
import org.apache.wayang.postgres.plugin.PostgresConversionsPlugin;
import org.apache.wayang.postgres.plugin.PostgresPlugin;

/**
 * Register for relevant components of this module.
 */
public class Postgres {

    private final static PostgresPlugin PLUGIN = new PostgresPlugin();

    private final static PostgresConversionsPlugin CONVERSIONS_PLUGIN = new PostgresConversionsPlugin();

    /**
     * Retrieve the {@link PostgresPlugin}.
     *
     * @return the {@link PostgresPlugin}
     */
    public static PostgresPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link PostgresConversionsPlugin}.
     *
     * @return the {@link PostgresConversionsPlugin}
     */
    public static PostgresConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
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
