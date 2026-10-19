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

package org.apache.wayang.presto;

import org.apache.wayang.presto.platform.PrestoPlatform;
import org.apache.wayang.presto.plugin.PrestoConversionsPlugin;
import org.apache.wayang.presto.plugin.PrestoPlugin;

/**
 * Entry point that exposes the relevant components of the Presto platform.
 *
 * <p>Typical usage:
 * <pre>{@code
 *   new WayangContext(config)
 *       .withPlugin(Java.basicPlugin())
 *       .withPlugin(Presto.plugin());
 * }</pre>
 */
public class Presto {

    private static final PrestoPlugin PLUGIN = new PrestoPlugin();

    private static final PrestoConversionsPlugin CONVERSIONS_PLUGIN = new PrestoConversionsPlugin();

    /**
     * @return the {@link PrestoPlugin} (operator mappings + channel conversions)
     */
    public static PrestoPlugin plugin() {
        return PLUGIN;
    }

    /**
     * @return the {@link PrestoConversionsPlugin} (channel conversions only)
     */
    public static PrestoConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }

    /**
     * @return the {@link PrestoPlatform}
     */
    public static PrestoPlatform platform() {
        return PrestoPlatform.getInstance();
    }

}
