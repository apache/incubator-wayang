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

package org.apache.wayang.core.optimizer.costs;

import java.util.HashMap;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.util.JsonSerializables;
import org.apache.wayang.core.util.JsonSerializer;

import java.util.Collection;
import java.util.List;
import org.apache.wayang.core.util.json.WayangJsonObj;

/**
 * This {@link EstimationContext} implementation just stores all required variables without any further logic.
 */
public class SimpleEstimationContext implements EstimationContext {

    private final CardinalityEstimate[] inputCardinalities, outputCardinalities;
    //TODO: change a for efficient Hashmap
    private final HashMap<String, Double> doubleProperties;

    private final int numExecutions;

    /**
     * Creates a new instance.
     */
    public SimpleEstimationContext(CardinalityEstimate[] inputCardinalities,
                                   CardinalityEstimate[] outputCardinalities,
                                   HashMap<String, Double> doubleProperties,
                                   int numExecutions) {
        this.inputCardinalities = inputCardinalities;
        this.outputCardinalities = outputCardinalities;
        this.doubleProperties = doubleProperties;
        this.numExecutions = numExecutions;
    }

    @Override
    public CardinalityEstimate[] getInputCardinalities() {
        return this.inputCardinalities;
    }

    @Override
    public CardinalityEstimate[] getOutputCardinalities() {
        return this.outputCardinalities;
    }

    @Override
    public double getDoubleProperty(String propertyKey, double fallback) {
        return this.doubleProperties.containsKey(propertyKey) ?
                this.doubleProperties.get(propertyKey) :
                fallback;
    }

    @Override
    public int getNumExecutions() {
        return this.numExecutions;
    }

    @Override
    public Collection<String> getPropertyKeys() {
        return this.doubleProperties.keySet();
    }

    /**
     * {@link JsonSerializer} for {@link SimpleEstimationContext}s.
     */
    public static JsonSerializer<SimpleEstimationContext> jsonSerializer =
            new JsonSerializer<SimpleEstimationContext>() {

                @Override
                public WayangJsonObj serialize(SimpleEstimationContext ctx) {
                    return EstimationContext.defaultSerializer.serialize(ctx);
                }

                @Override
                public SimpleEstimationContext deserialize(WayangJsonObj json) {
                    return this.deserialize(json, SimpleEstimationContext.class);
                }

                @Override
                public SimpleEstimationContext deserialize(WayangJsonObj json, Class<? extends SimpleEstimationContext> cls) {
                    final List<CardinalityEstimate> inCards = JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("inCards"),
                            CardinalityEstimate.class
                    );
                    final List<CardinalityEstimate> outCards = JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("outCards"),
                            CardinalityEstimate.class
                    );

                    final HashMap<String, Double> doubleProperties = new HashMap<String, Double>();
                    final WayangJsonObj doublePropertiesJson = json.optionalWayangJsonObj("properties");
                    if (doublePropertiesJson != null) {
                        for (String key : doublePropertiesJson.keySet()) {
                            doubleProperties.put(key, doublePropertiesJson.getDouble(key));
                        }
                    }

                    final int numExecutions = json.getInt("executions");

                    return new SimpleEstimationContext(
                            inCards.toArray(new CardinalityEstimate[inCards.size()]),
                            outCards.toArray(new CardinalityEstimate[outCards.size()]),
                            doubleProperties,
                            numExecutions
                    );
                }
            };
}
