package org.apache.wayang.graphchi;

import org.apache.wayang.graphchi.platform.GraphChiPlatform;
import org.apache.wayang.graphchi.plugin.GraphChiPlugin;

/**
 * Register for relevant components of this module.
 */
public class GraphChi {

    private final static GraphChiPlugin PLUGIN = new GraphChiPlugin();

    /**
     * Retrieve the {@link GraphChiPlugin}.
     *
     * @return the {@link GraphChiPlugin}
     */
    public static GraphChiPlugin plugin() {
        return PLUGIN;
    }


    /**
     * Retrieve the {@link GraphChiPlatform}.
     *
     * @return the {@link GraphChiPlatform}
     */
    public static GraphChiPlatform platform() {
        return GraphChiPlatform.getInstance();
    }

}
