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

package org.apache.wayang.ml.costs;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.costs.DefaultEstimatableCost;
import org.apache.wayang.core.optimizer.costs.EstimatableCost;
import org.apache.wayang.core.optimizer.costs.EstimatableCostFactory;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.OrtMLModel;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;

import ai.onnxruntime.OrtException;

/**
 * Default {@link EstimatableCost} for pointwise/cost-based ML models. <br>
 * Takes config {@code wayang.ml.experience.enabled}
 */
public class DefaultPointwiseCost extends DefaultEstimatableCost {
    private static final Logger logger = LogManager.getLogger(DefaultPointwiseCost.class);

    public static class Factory implements EstimatableCostFactory {
        private final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());

        @Override
        public EstimatableCost makeCost() {
            return new DefaultPointwiseCost(encoder);
        }
    }
    
    private final TreeEncoder encoder;

    public DefaultPointwiseCost(TreeEncoder encoder) {
        this.encoder = encoder;
    }

    @Override
    public PlanImplementation pickBestExecutionPlan(final Collection<PlanImplementation> executionPlans,
            final ExecutionPlan existingPlan, final Set<Channel> openChannels,
            final Set<ExecutionStage> executedStages) {

        final Map<PlanImplementation, Double> planCostMapping = executionPlans.stream()
                .collect(Collectors.toMap(Function.identity(), this::getCost));

        final PlanImplementation bestPlanImplementation = executionPlans.stream()
                .min(Comparator.comparingDouble(planCostMapping::get))
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));

        final Configuration config = bestPlanImplementation.getOptimizationContext().getConfiguration();

        if (config.getOptionalBooleanProperty("wayang.ml.experience.enabled").orElse(false)) {
            final TreeNode encodedPlan = encoder.encode(bestPlanImplementation);
            config.setProperty("wayang.ml.experience.with-platforms", encodedPlan.toString());
        }

        return bestPlanImplementation;
    }

    /**
     * Estimates the runtime cost for a given plan.
     * 
     * @param plan
     * @return
     */
    public Double getCost(final PlanImplementation plan) {
        try {
            final Configuration config = plan.getOptimizationContext().getConfiguration();
            final OrtMLModel model = OrtMLModel.getInstance(config);
            final TreeNode encodedOne = encoder.encode(plan);
            final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple1 = OrtTensorEncoder.encode(encodedOne);
            final double cost = Math.exp(model.runModel(tuple1)) - 1;

            return cost;
        } catch (final OrtException e) {
            logger.warn("Failed to estimate ML cost for plan {" + plan + "}. Falling back to MAX_VALUE.", e);
            return Double.MAX_VALUE;
        }
    }
}
