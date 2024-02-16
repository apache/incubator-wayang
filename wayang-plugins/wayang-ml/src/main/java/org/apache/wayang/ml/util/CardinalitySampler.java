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

package org.apache.wayang.ml.util;

import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.api.Configuration;

import java.io.File;
import java.io.FileWriter;

public class CardinalitySampler {

    public static void configure(
            Configuration config,
            WayangPlan plan,
            String filePath){
        String path = filePath + plan.hashCode() + "-cardinalities.json";
        config.setProperty("wayang.core.log.enabled", "true");
        config.setProperty("wayang.core.log.cardinalities", path);
        config.setProperty("wayang.core.optimizer.instrumentation", "org.apache.wayang.core.profiling.FullInstrumentationStrategy");

        // clear previous measurements from file
        try {
            File f = new File(path);
            if(f.exists() && !f.isDirectory()) {
               new FileWriter(path, false).close();
            }
        } catch (Exception e) {
            return;
        }
    }
}
