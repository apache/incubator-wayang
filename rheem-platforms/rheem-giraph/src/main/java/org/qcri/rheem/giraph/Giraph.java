package org.qcri.rheem.giraph;

import org.qcri.rheem.giraph.platform.GiraphPlatform;
import org.qcri.rheem.giraph.plugin.GiraphPlugin;

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
