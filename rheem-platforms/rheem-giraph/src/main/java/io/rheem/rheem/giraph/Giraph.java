package io.rheem.rheem.giraph;

import io.rheem.rheem.giraph.platform.GiraphPlatform;
import io.rheem.rheem.giraph.plugin.GiraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Giraph {
    private final static GiraphPlugin PLUGIN = new GiraphPlugin();

    /**
     * Retrieve the {@link GiraphPlugin}.
     *
     * @return the {@link GiraphPlugin}
     */
    public static GiraphPlugin plugin() {
        return PLUGIN;
    }


    /**
     * Retrieve the {@link GiraphPlatform}.
     *
     * @return the {@link GiraphPlatform}
     */
    public static GiraphPlatform platform() {
        return GiraphPlatform.getInstance();
    }
}
