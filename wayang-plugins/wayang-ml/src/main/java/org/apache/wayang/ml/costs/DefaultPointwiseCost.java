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
import java.util.Set;

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
import org.apache.wayang.ml.encoding.OrtMLModel;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;


/**
 * Default {@link EstimatableCost} for pointwise/cost-based ML models.
 * <br> Takes config {@code wayang.ml.experience.enabled}
 */
public class DefaultPointwiseCost extends DefaultEstimatableCost {
    public static class Factory implements EstimatableCostFactory {
        @Override
        public EstimatableCost makeCost() {
            return new DefaultPointwiseCost();
        }
    }

    @Override
    public PlanImplementation pickBestExecutionPlan(final Collection<PlanImplementation> executionPlans,
            final ExecutionPlan existingPlan, final Set<Channel> openChannels, final Set<ExecutionStage> executedStages) {

        final PlanImplementation bestPlanImplementation = executionPlans.stream().reduce((p1, p2) -> {
            try {
                final Configuration config = p1.getOptimizationContext().getConfiguration();

                final OrtMLModel model = OrtMLModel.getInstance(config);

                final TreeNode encodedOne = TreeEncoder.encode(p1);
                final TreeNode encodedTwo = TreeEncoder.encode(p2);

                final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple1 = OrtTensorEncoder.encode(encodedOne);
                final Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple2 = OrtTensorEncoder.encode(encodedTwo);

                final double leftCost = Math.exp(model.runModel(tuple1)) - 1;
                final double rightCost = Math.exp(model.runModel(tuple2)) - 1;

                return leftCost < rightCost ? p1 : p2;
            } catch (final Exception e) {
                e.printStackTrace();
                return p1;
            }
        }).orElseThrow(() -> new WayangException("Could not find an execution plan."));

        final Configuration config = bestPlanImplementation.getOptimizationContext().getConfiguration();

        if (config.getBooleanProperty("wayang.ml.experience.enabled")) {
            final TreeNode encodedPlan = TreeEncoder.encode(bestPlanImplementation);
            config.setProperty("wayang.ml.experience.with-platforms", encodedPlan.toString());
        }

        return bestPlanImplementation;
    }
}
