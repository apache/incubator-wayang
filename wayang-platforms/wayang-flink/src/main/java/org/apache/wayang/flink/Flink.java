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

package org.apache.wayang.flink;


import org.apache.wayang.flink.platform.FlinkPlatform;
import org.apache.wayang.flink.plugin.FlinkBasicPlugin;
import org.apache.wayang.flink.plugin.FlinkConversionPlugin;
import org.apache.wayang.flink.plugin.FlinkGraphPlugin;

/**
 * Register for relevant components of this module.
 */
public class Flink {

    private final static FlinkBasicPlugin PLUGIN = new FlinkBasicPlugin();

    private final static FlinkGraphPlugin GRAPH_PLUGIN = new FlinkGraphPlugin();

    private final static FlinkConversionPlugin CONVERSION_PLUGIN = new FlinkConversionPlugin();

    /**
     * Retrieve the {@link FlinkBasicPlugin}.
     *
     * @return the {@link FlinkBasicPlugin}
     */
    public static FlinkBasicPlugin basicPlugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkGraphPlugin}.
     *
     * @return the {@link FlinkGraphPlugin}
     */
    public static FlinkGraphPlugin graphPlugin() {
        return GRAPH_PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkConversionPlugin}.
     *
     * @return the {@link FlinkConversionPlugin}
     */
    public static FlinkConversionPlugin conversionPlugin() {
        return CONVERSION_PLUGIN;
    }

    /**
     * Retrieve the {@link FlinkPlatform}.
     *
     * @return the {@link FlinkPlatform}
     */
    public static FlinkPlatform platform() {
        return FlinkPlatform.getInstance();
    }
}
