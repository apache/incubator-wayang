package org.qcri.rheem.spark;

import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.spark.plugin.SparkBasicPlugin;
import org.qcri.rheem.spark.plugin.SparkConversionPlugin;
import org.qcri.rheem.spark.plugin.SparkGraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Spark {

    private final static SparkBasicPlugin PLUGIN = new SparkBasicPlugin();

    private final static SparkGraphPlugin GRAPH_PLUGIN = new SparkGraphPlugin();

    private final static SparkConversionPlugin CONVERSION_PLUGIN = new SparkConversionPlugin();

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
     * Retrieve the {@link SparkConversionPlugin}.
     *
     * @return the {@link SparkConversionPlugin}
     */
    public static SparkConversionPlugin conversionPlugin() {
        return CONVERSION_PLUGIN;
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