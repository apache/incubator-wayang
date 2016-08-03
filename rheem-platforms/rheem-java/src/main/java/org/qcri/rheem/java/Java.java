package org.qcri.rheem.java;

import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.java.plugin.JavaBasicPlugin;

/**
 * Register for relevant components of this module.
 */
public class Java {

    private final static JavaBasicPlugin PLUGIN = new JavaBasicPlugin();

    /**
     * Retrieve the {@link JavaBasicPlugin}.
     *
     * @return the {@link JavaBasicPlugin}
     */
    public static JavaBasicPlugin basicPlugin() {
        return PLUGIN;
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
