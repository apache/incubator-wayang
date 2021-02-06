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

package org.apache.wayang.basic;

import org.apache.wayang.basic.plugin.WayangBasic;
import org.apache.wayang.basic.plugin.WayangBasicGraph;

/**
 * Register for plugins in the module.
 */
public class WayangBasics {

    private static final WayangBasic DEFAULT_PLUGIN = new WayangBasic();

    private static final WayangBasicGraph GRAPH_PLUGIN = new WayangBasicGraph();

    public static WayangBasic defaultPlugin() {
        return DEFAULT_PLUGIN;
    }

    public static WayangBasicGraph graphPlugin() {
        return GRAPH_PLUGIN;
    }

}
