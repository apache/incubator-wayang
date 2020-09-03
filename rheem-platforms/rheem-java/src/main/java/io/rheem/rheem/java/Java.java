package io.rheem.rheem.java;

import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.java.plugin.JavaBasicPlugin;
import io.rheem.rheem.java.plugin.JavaChannelConversionPlugin;
import io.rheem.rheem.java.plugin.JavaGraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Java {

    private final static JavaBasicPlugin PLUGIN = new JavaBasicPlugin();

    private final static JavaGraphPlugin GRAPH_PLUGIN = new JavaGraphPlugin();

    private final static JavaChannelConversionPlugin CONVERSION_PLUGIN = new JavaChannelConversionPlugin();

    /**
     * Retrieve the {@link JavaBasicPlugin}.
     *
     * @return the {@link JavaBasicPlugin}
     */
    public static JavaBasicPlugin basicPlugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link JavaGraphPlugin}.
     *
     * @return the {@link JavaGraphPlugin}
     */
    public static JavaGraphPlugin graphPlugin() {
        return GRAPH_PLUGIN;
    }

    /**
     * Retrieve the {@link JavaChannelConversionPlugin}.
     *
     * @return the {@link JavaChannelConversionPlugin}
     */
    public static JavaChannelConversionPlugin channelConversionPlugin() {
        return CONVERSION_PLUGIN;
    }

    /**
     * Retrieve the {@link JavaPlatform}.
     *
     * @return the {@link JavaPlatform}
     */
    public static JavaPlatform platform() {
        return JavaPlatform.getInstance();
    }

}
