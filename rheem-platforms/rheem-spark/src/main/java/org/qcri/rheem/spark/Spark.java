package org.qcri.rheem.spark;

import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.spark.plugin.SparkBasicPlugin;
import org.qcri.rheem.spark.plugin.SparkGraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Spark {

    private final static SparkBasicPlugin PLUGIN = new SparkBasicPlugin();

    private final static SparkGraphPlugin GRAPH_PLUGIN = new SparkGraphPlugin();

    /**
     * Retrieve the {@link SparkBasicPlugin}.
     *
     * @return the {@link SparkBasicPlugin}
     */
    public static SparkBasicPlugin basicPlugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link SparkGraphPlugin}.
     *
     * @return the {@link SparkGraphPlugin}
     */
    public static SparkGraphPlugin graphPlugin() {
        return GRAPH_PLUGIN;
    }

    /**
     * Retrieve the {@link SparkPlatform}.
     *
     * @return the {@link SparkPlatform}
     */
    public static SparkPlatform platform() {
        return SparkPlatform.getInstance();
    }

}