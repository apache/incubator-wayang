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
import org.apache.wayang.core.optimizer.costs.EstimationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.LoadProfileToTimeConverter;
import org.apache.wayang.core.optimizer.costs.SimpleEstimationContext;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.JsonSerializer;

import java.util.Collection;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * This class groups {@link AtomicExecution}s with a common {@link EstimationContext} and {@link Platform}.
 */
public class AtomicExecutionGroup {

    /**
     * The common {@link EstimationContext}.
     */
    private EstimationContext estimationContext;

    /**
     * The common {@link Platform} for all {@link #atomicExecutions}.
     */
    private Platform platform;

    /**
     * The {@link AtomicExecution}s.
     */
    private Collection<AtomicExecution> atomicExecutions;

    /**
     * The {@link Configuration} that provides estimation information.
     */
    private Configuration configuration;

    /**
     * Caches the {@link LoadProfileToTimeConverter} for this instance.
     */
    private LoadProfileToTimeConverter loadProfileToTimeConverterCache;

    public AtomicExecutionGroup(EstimationContext estimationContext,
                                Platform platform,
                                Configuration configuration,
                                Collection<AtomicExecution> atomicExecutions) {
        this.estimationContext = estimationContext;
        this.platform = platform;
        this.atomicExecutions = atomicExecutions;
        this.configuration = configuration;
    }

    /**
     * Estimate the {@link LoadProfile} for all {@link AtomicExecution}s in this instance.
     *
     * @return the {@link LoadProfile}
     */
    public LoadProfile estimateLoad() {
        return this.estimateLoad(this.estimationContext);
    }

    /**
     * Estimate the {@link LoadProfile} for all {@link AtomicExecution}s in this instance in the light of
     * a specific {@link EstimationContext}.
     *
     * @return the {@link LoadProfile}
     */
    public LoadProfile estimateLoad(EstimationContext estimationContext) {
        return this.atomicExecutions.stream()
                .map(execution -> execution.estimateLoad(estimationContext))
                .reduce(LoadProfile::plus)
                .orElse(LoadProfile.emptyLoadProfile);
    }

    /**
     * Estimate the {@link TimeEstimate} for all {@link AtomicExecution}s in this instance in the light of the
     * given {@link EstimationContext}.
     *
     * @param estimationContext that provides estimation parameters
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate estimateExecutionTime(EstimationContext estimationContext) {
        if (this.loadProfileToTimeConverterCache == null) {
            this.loadProfileToTimeConverterCache = this.configuration
                    .getLoadProfileToTimeConverterProvider()
                    .provideFor(this.platform);
        }
        return this.loadProfileToTimeConverterCache.convert(this.estimateLoad(estimationContext));
    }

    /**
     * Estimate the {@link TimeEstimate} for all {@link AtomicExecution}s in this instance.
     *
     * @return the {@link TimeEstimate}
     */
    public TimeEstimate estimateExecutionTime() {
        return this.estimateExecutionTime(this.estimationContext);
    }

    public EstimationContext getEstimationContext() {
        return this.estimationContext;
    }

    public Platform getPlatform() {
        return this.platform;
    }

    public Collection<AtomicExecution> getAtomicExecutions() {
        return this.atomicExecutions;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %s]", this.getClass().getSimpleName(), this.platform, this.atomicExecutions);
    }

    /**
     * {@link JsonSerializer} implementation for {@link AtomicExecutionGroup}s.
     */
    public static class Serializer implements JsonSerializer<AtomicExecutionGroup> {

        private final Configuration configuration;

        /**
         * Creates a new instance.
         *
         * @param configuration is required for deserialization; can otherwise be {@code null}
         */
        public Serializer(Configuration configuration) {
            this.configuration = configuration;
        }


        @Override
        public WayangJsonObj serialize(AtomicExecutionGroup aeg) {
            AtomicExecution.KeyOrLoadSerializer atomicExecutionSerializer =
                    new AtomicExecution.KeyOrLoadSerializer(null, aeg.estimationContext);
            return new WayangJsonObj()
                    .put("ctx", JsonSerializables.serialize(aeg.estimationContext, false, EstimationContext.defaultSerializer))
                    .put("platform", JsonSerializables.serialize(aeg.platform, true, Platform.jsonSerializer))
                    .put("executions", JsonSerializables.serializeAll(aeg.atomicExecutions, false, atomicExecutionSerializer));
        }

        @Override
        public AtomicExecutionGroup deserialize(WayangJsonObj json, Class<? extends AtomicExecutionGroup> cls) {
            return new AtomicExecutionGroup(
                    JsonSerializables.deserialize(
                            json.getJSONObject("ctx"),
                            SimpleEstimationContext.jsonSerializer,
                            SimpleEstimationContext.class
                    ),
                    JsonSerializables.deserialize(
                            json.getJSONObject("platform"),
                            Platform.jsonSerializer
                    ),
                    this.configuration,
                    JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("executions"),
                            new AtomicExecution.KeyOrLoadSerializer(this.configuration, null),
                            AtomicExecution.class
                    )
            );
        }
    }

}
