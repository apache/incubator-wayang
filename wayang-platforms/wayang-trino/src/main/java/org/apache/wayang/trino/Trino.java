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

package org.apache.wayang.trino;


import org.apache.wayang.trino.platform.TrinoPlatform;
import org.apache.wayang.trino.plugin.TrinoConversionsPlugin;
import org.apache.wayang.trino.plugin.TrinoPlugin;

/**
 * Register for relevant components of this module.
 */
public class Trino {

    private final static TrinoPlugin PLUGIN = new TrinoPlugin();

    private final static TrinoConversionsPlugin CONVERSIONS_PLUGIN = new TrinoConversionsPlugin();

    /**
     * Retrieve the {@link TrinoPlugin}.
     *
     * @return the {@link TrinoPlugin}
     */
    public static TrinoPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link TrinoConversionsPlugin}.
     *
     * @return the {@link TrinoConversionsPlugin}
     */
    public static TrinoConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link TrinoPlatform}.
     *
     * @return the {@link TrinoPlatform}
     */
    public static TrinoPlatform platform() {
        return TrinoPlatform.getInstance();
    }

}
