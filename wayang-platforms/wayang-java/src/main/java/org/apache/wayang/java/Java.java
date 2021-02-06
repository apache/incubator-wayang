/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.java;

import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.java.plugin.JavaBasicPlugin;
import org.apache.wayang.java.plugin.JavaChannelConversionPlugin;
import org.apache.wayang.java.plugin.JavaGraphPlugin;

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
