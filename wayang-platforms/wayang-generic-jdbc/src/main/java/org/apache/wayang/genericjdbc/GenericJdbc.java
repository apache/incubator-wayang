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

package org.apache.wayang.genericjdbc;


import org.apache.wayang.genericjdbc.platform.GenericJdbcPlatform;
import org.apache.wayang.genericjdbc.plugin.GenericJdbcConversionsPlugin;
import org.apache.wayang.genericjdbc.plugin.GenericJdbcPlugin;

/**
 * Register for relevant components of this module.
 */
public class GenericJdbc {

    private final static GenericJdbcPlugin PLUGIN = new GenericJdbcPlugin();

    private final static GenericJdbcConversionsPlugin CONVERSIONS_PLUGIN = new GenericJdbcConversionsPlugin();

    /**
     * Retrieve the {@link GenericJdbcPlugin}.
     *
     * @return the {@link GenericJdbcPlugin}
     */
    public static GenericJdbcPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link GenericJdbcConversionsPlugin}.
     *
     * @return the {@link GenericJdbcConversionsPlugin}
     */
    public static GenericJdbcConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link GenericJdbcPlatform}.
     *
     * @return the {@link GenericJdbcPlatform}
     */
    public static GenericJdbcPlatform platform() {
        return GenericJdbcPlatform.getInstance();
    }

}
