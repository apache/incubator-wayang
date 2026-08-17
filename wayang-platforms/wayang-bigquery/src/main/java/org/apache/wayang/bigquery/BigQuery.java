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

package org.apache.wayang.bigquery;


import org.apache.wayang.bigquery.platform.BigQueryPlatform;
import org.apache.wayang.bigquery.plugin.BigQueryConversionsPlugin;
import org.apache.wayang.bigquery.plugin.BigQueryPlugin;

/**
 * Register for relevant components of this module.
 */
public class BigQuery {

    private final static BigQueryPlugin PLUGIN = new BigQueryPlugin();

    private final static BigQueryConversionsPlugin CONVERSIONS_PLUGIN = new BigQueryConversionsPlugin();

    /**
     * Retrieve the {@link BigQueryPlugin}.
     *
     * @return the {@link BigQueryPlugin}
     */
    public static BigQueryPlugin plugin() {
        return PLUGIN;
    }

    /**
     * Retrieve the {@link BigQueryConversionsPlugin}.
     *
     * @return the {@link BigQueryConversionsPlugin}
     */
    public static BigQueryConversionsPlugin conversionPlugin() {
        return CONVERSIONS_PLUGIN;
    }


    /**
     * Retrieve the {@link BigQueryPlatform}.
     *
     * @return the {@link BigQueryPlatform}
     */
    public static BigQueryPlatform platform() {
        return BigQueryPlatform.getInstance();
    }

}
