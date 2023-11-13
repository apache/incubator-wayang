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

package org.apache.wayang.spark;

import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.spark.plugin.SparkBasicPlugin;
import org.apache.wayang.spark.plugin.SparkConversionPlugin;
import org.apache.wayang.spark.plugin.SparkGraphPlugin;
import org.apache.wayang.spark.plugin.SparkMLPlugin;

/**
 * Register for relevant components of this module.
 */
public class Spark {

    private final static SparkBasicPlugin PLUGIN = new SparkBasicPlugin();

    private final static SparkGraphPlugin GRAPH_PLUGIN = new SparkGraphPlugin();

    private final static SparkConversionPlugin CONVERSION_PLUGIN = new SparkConversionPlugin();

    private final static SparkMLPlugin ML_PLUGIN = new SparkMLPlugin();

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
     * Retrieve the {@link SparkMLPlugin}.
     *
     * @return the {@link SparkMLPlugin}
     */
    public static SparkMLPlugin mlPlugin() {
        return ML_PLUGIN;
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
