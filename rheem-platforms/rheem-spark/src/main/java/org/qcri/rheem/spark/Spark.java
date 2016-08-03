package org.qcri.rheem.spark;

import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.spark.plugin.SparkBasicPlugin;

/**
 * Register for relevant components of this module.
 */
public class Spark {

    private final static SparkBasicPlugin PLUGIN = new SparkBasicPlugin();

    /**
     * Retrieve the {@link SparkBasicPlugin}.
     *
     * @return the {@link SparkBasicPlugin}
     */
    public static SparkBasicPlugin basicPlugin() {
        return PLUGIN;
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