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

package org.apache.wayang.core.api;

import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plugin.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiConfigurationWayangContext {

    private static final Logger logger = LoggerFactory.getLogger(MultiConfigurationWayangContext.class);

    List<Configuration> configurationList;

    public MultiConfigurationWayangContext(List<Configuration> configurationList) {
        this.configurationList = configurationList;
    }

    public MultiConfigurationWayangContext with(Plugin plugin) {
        return this.withPlugin(plugin);
    }

    public MultiConfigurationWayangContext withPlugin(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    public void register(Plugin plugin) {
        configurationList.forEach(plugin::configure);
    }

    public void execute(WayangPlan wayangPlan, String... udfJars) {
        // Create an ExecutorService with a fixed thread pool, you might want to set the number of threads based on your system resources or a configurable parameter
        ExecutorService executor = Executors.newFixedThreadPool(configurationList.size());

        // Submit each execution to the executor
        for (Configuration configuration : configurationList) {
            executor.submit(() -> new WayangContext(configuration).execute(wayangPlan, udfJars));
        }

        // Shutdown the executor so it doesn't accept new tasks
        executor.shutdown();

        try {
            // Wait for the tasks to complete
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                executor.shutdownNow(); // Forcefully terminate tasks
            }
        } catch (InterruptedException e) {
            // Handle the interruption
            executor.shutdownNow(); // Forcefully terminate tasks
            Thread.currentThread().interrupt();
        }
    }
}
