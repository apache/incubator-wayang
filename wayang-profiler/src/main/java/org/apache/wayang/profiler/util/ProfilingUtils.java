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

package org.apache.wayang.profiler.util;

import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.logging.log4j.LogManager;

/**
 * Utilities to fake Wayang internals etc..
 */
public class ProfilingUtils {

    private ProfilingUtils() {
    }

    /**
     * Provides a {@link Job}.
     *
     * @param udfJars paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    public static Job fakeJob(String... udfJars) {
        return new WayangContext().createJob("Fake job", new WayangPlan(), udfJars);
    }

    /**
     * Provides a {@link SparkExecutor}.
     *
     * @param udfJars paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    public static SparkExecutor fakeSparkExecutor(String... udfJars) {
        return (SparkExecutor) SparkPlatform.getInstance().createExecutor(fakeJob(udfJars));
    }

    /**
     * Provides a {@link JavaExecutor}.
     */
    public static JavaExecutor fakeJavaExecutor() {
        return (JavaExecutor) JavaPlatform.getInstance().createExecutor(fakeJob());
    }

    /**
     * Puts the current {@link Thread} to sleep for a given number of milliseconds. Notifies the user via the
     * {@link System#out}.
     */
    public static void sleep(long millis) {
        try {
            System.out.printf("Sleeping for %s.\n", Formats.formatDuration(millis));
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LogManager.getLogger(ProfilingUtils.class).error("Sleep interrupted.", e);
        }
    }
}
