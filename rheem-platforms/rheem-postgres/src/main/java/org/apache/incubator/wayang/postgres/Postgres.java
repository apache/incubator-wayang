package io.rheem.rheem.postgres;


import io.rheem.rheem.postgres.platform.PostgresPlatform;
import io.rheem.rheem.postgres.plugin.PostgresConversionsPlugin;
import io.rheem.rheem.postgres.plugin.PostgresPlugin;

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
