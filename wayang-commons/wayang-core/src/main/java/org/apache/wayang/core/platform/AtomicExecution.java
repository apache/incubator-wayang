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

import org.apache.commons.lang3.SerializationException;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.costs.ConstantLoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.EstimationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfile;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.JsonSerializer;
import org.apache.wayang.core.util.json.WayangJsonArray;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * An atomic execution describes the smallest work unit considered by Wayang's cost model.
 */
public class AtomicExecution {

    /**
     * Estimates the {@link LoadProfile} of the execution unit.
     */
    private LoadProfileEstimator loadProfileEstimator;

    /**
     * Creates a new instance with the given {@link LoadProfileEstimator}.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator}
     */
    public AtomicExecution(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    /**
     * Estimates the {@link LoadProfile} for this instance under a given {@link EstimationContext}.
     *
     * @param context the {@link EstimationContext}
     * @return the {@link LoadProfile}
     */
    public LoadProfile estimateLoad(EstimationContext context) {
        return this.loadProfileEstimator.estimate(context);
    }

    /**
     * This {@link JsonSerializer} stores the given instances via their {@link org.apache.wayang.core.api.Configuration}
     * key, if any, or else by the {@link LoadProfile} that they estimate.
     */
    public static class KeyOrLoadSerializer implements JsonSerializer<AtomicExecution> {

        private final Configuration configuration;

        private final EstimationContext estimationContext;

        /**
         * Creates a new instance.
         *
         * @param configuration     required for deserialization; can otherwise be {@code null}
         * @param estimationContext of the enclosing {@link AtomicExecutionGroup};
         *                          required for serialization; can otherwise be {@code null}
         */
        public KeyOrLoadSerializer(Configuration configuration, EstimationContext estimationContext) {
            this.configuration = configuration;
            this.estimationContext = estimationContext;
        }

        @Override
        public WayangJsonObj serialize(AtomicExecution atomicExecution) {
            WayangJsonArray estimators = new WayangJsonArray();
            this.serialize(atomicExecution.loadProfileEstimator, estimators);
            return new WayangJsonObj().put("estimators", JsonSerializables.serialize(estimators, false));
        }

        private void serialize(LoadProfileEstimator estimator, WayangJsonArray collector) {
            WayangJsonObj json = new WayangJsonObj();
            if (estimator.getConfigurationKey() != null) {
                json.put("key", estimator.getConfigurationKey());
            } else {
                final LoadProfile loadProfile = estimator.estimate(this.estimationContext);
                json.put("load", JsonSerializables.serialize(loadProfile, false));
            }
            collector.put(json);
            for (LoadProfileEstimator nestedEstimator : estimator.getNestedEstimators()) {
                this.serialize(nestedEstimator, collector);
            }
        }

        @Override
        public AtomicExecution deserialize(WayangJsonObj json) {
            return this.deserialize(json, AtomicExecution.class);
        }

        @Override
        public AtomicExecution deserialize(WayangJsonObj json, Class<? extends AtomicExecution> cls) {
            final WayangJsonArray estimators = json.getJSONArray("estimators");
            if (estimators.length() < 1) {
                throw new IllegalStateException("Expected at least one serialized estimator.");
            }
            // De-serialize the main estimator.
            final WayangJsonObj mainEstimatorJson = estimators.getJSONObject(0);
            LoadProfileEstimator mainEstimator = this.deserializeEstimator(mainEstimatorJson);

            // De-serialize nested estimators.
            for (int i = 1; i < estimators.length(); i++) {
                mainEstimator.nest(this.deserializeEstimator(estimators.getJSONObject(i)));
            }

            return new AtomicExecution(mainEstimator);
        }

        /**
         * Deserialize a {@link LoadProfileEstimator} according to {@link #serialize(LoadProfileEstimator, WayangJsonArray)}.
         *
         * @param wayangJsonObj that should be deserialized
         * @return the {@link LoadProfileEstimator}
         */
        private LoadProfileEstimator deserializeEstimator(WayangJsonObj wayangJsonObj) {
            if (wayangJsonObj.has("key")) {
                final String key = wayangJsonObj.getString("key");
                final LoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(key, this.configuration);
                if (estimator == null) {
                    throw new SerializationException("Could not create estimator for key " + key);
                }
                return estimator;
            } else if (wayangJsonObj.has("load")) {
                final LoadProfile load = JsonSerializables.deserialize(wayangJsonObj.getJSONObject("load"), LoadProfile.class);
                return new ConstantLoadProfileEstimator(load);
            }
            throw new SerializationException(String.format("Cannot deserialize load estimator from %s.",
                wayangJsonObj));
        }
    }

    /**
     * Retrieve the {@link LoadProfileEstimator} encapsulated by this instance.
     *
     * @return the {@link LoadProfileEstimator}
     */
    public LoadProfileEstimator getLoadProfileEstimator() {
        return this.loadProfileEstimator;
    }

    /**
     * Change the {@link LoadProfileEstimator} encapsulated by this instance.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator}
     */
    public void setLoadProfileEstimator(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append('[');
        if (this.loadProfileEstimator.getConfigurationKey() != null) {
            sb.append(this.loadProfileEstimator.getConfigurationKey());
        } else {
            sb.append(this.loadProfileEstimator);
        }
        if (!this.loadProfileEstimator.getNestedEstimators().isEmpty()) {
            sb.append('+').append(this.loadProfileEstimator.getNestedEstimators().size()).append(" nested");
        }
        sb.append(']');
        return sb.toString();
    }
}
