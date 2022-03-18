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

package org.apache.wayang.core.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.executionplan.PlatformExecution;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.JsonSerializer;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * A platform describes an execution engine that executes {@link ExecutionOperator}s.
 */
public abstract class Platform {

    private final String name, configName;

    /**
     * Loads a specific {@link Platform} implementation. For platforms to interoperate with this method, they must
     * provide a {@code static}, parameterless method {@code getInstance()} that returns their singleton instance.
     *
     * @param platformClassName the class name of the {@link Platform}
     * @return the {@link Platform} instance
     */
    public static Platform load(String platformClassName) {
        try {
            return ReflectionUtils.executeStaticArglessMethod(platformClassName, "getInstance");
        } catch (Exception e) {
            throw new WayangException("Could not load platform: " + platformClassName, e);
        }
    }

    protected Platform(String name, String configName) {
        this.name = name;
        this.configName = configName;
        this.configureDefaults(Configuration.getDefaultConfiguration());
    }

    /**
     * Configure default settings for this instance, e.g., to be able to create {@link LoadProfileToTimeConverter}s.
     *
     * @param configuration that should be configured
     */
    protected abstract void configureDefaults(Configuration configuration);

    /**
     * <i>Shortcut.</i> Creates an {@link Executor} using the {@link #getExecutorFactory()}.
     *
     * @return the {@link Executor}
     */
    public Executor createExecutor(Job job) {
        return this.getExecutorFactory().create(job);
    }

    public abstract Executor.Factory getExecutorFactory();

    public String getName() {
        return this.name;
    }

    /**
     * Retrieve the name of this instance as it is used in {@link Configuration} keys.
     *
     * @return the configuration name of this instance
     */
    public String getConfigurationName() {
        return this.configName;
    }

    // TODO: Return some more descriptors about the state of the platform (e.g., available machines, RAM, ...)?

    @Override
    public String toString() {
        return String.format("Platform[%s]", this.getName());
    }

    /**
     * Tells whether the given constellation of producing and consuming {@link ExecutionTask}, linked by the
     * {@link Channel} can be handled within a single {@link PlatformExecution} of this {@link Platform}
     *
     * @param producerTask an {@link ExecutionTask} running on this {@link Platform}
     * @param channel      links the {@code producerTask} and {@code consumerTask}
     * @param consumerTask an  {@link ExecutionTask} running on this {@link Platform}
     * @return whether the {@link ExecutionTask}s can be executed in a single {@link PlatformExecution}
     */
    public boolean isSinglePlatformExecutionPossible(ExecutionTask producerTask, Channel channel, ExecutionTask consumerTask) {
        assert producerTask.getOperator().getPlatform() == this;
        assert consumerTask.getOperator().getPlatform() == this;
        assert channel.getProducer() == producerTask;
        assert channel.getConsumers().contains(consumerTask);

        // Overwrite as necessary.
        return true;
    }

    /**
     * @return a default {@link LoadProfileToTimeConverter}
     */
    public abstract LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration);

    /**
     * Creates a {@link TimeToCostConverter} for this instance.
     *
     * @param configuration configures the {@link TimeToCostConverter}
     * @return the {@link TimeToCostConverter}
     */
    public abstract TimeToCostConverter createTimeToCostConverter(Configuration configuration);

    /**
     * Warm up this instance.
     *
     * @param configuration used to warm up
     */
    public void warmUp(Configuration configuration) {
        // Do nothing by default.
    }

    /**
     * Get the time necessary to initialize this instance and use it for execution.
     *
     * @return the milliseconds required to initialize this instance
     */
    public long getInitializeMillis(Configuration configuration) {
        return 0L;
    }

    /**
     * Default {@link JsonSerializer} implementation that stores the {@link Class} of the instance and then
     * tries to deserialize by invoking the static {@code getInstance()} method.
     */
    public static final JsonSerializer<Platform> jsonSerializer = new JsonSerializer<Platform>() {

        @Override
        public WayangJsonObj serialize(Platform platform) {
            // Enforce polymorph serialization.
            return JsonSerializables.addClassTag(platform, new WayangJsonObj());
        }

        @Override
        public Platform deserialize(WayangJsonObj json, Class<? extends Platform> cls) {
            return ReflectionUtils.evaluate(cls.getCanonicalName() + ".getInstance()");
        }
    };
}
