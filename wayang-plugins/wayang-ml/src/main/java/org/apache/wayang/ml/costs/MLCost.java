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

import org.apache.wayang.core.optimizer.costs.EstimatableCost;
import org.apache.wayang.core.optimizer.costs.EstimatableCostFactory;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.enumeration.LoopImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.ml.OrtMLModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.List;

public class MLCost implements EstimatableCost {
    public EstimatableCostFactory getFactory() {
        return new Factory();
    }

public static class Factory implements EstimatableCostFactory {
        @Override public EstimatableCost makeCost() {
            return new MLCost();
        }
    }

    @Override public ProbabilisticDoubleInterval getEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);
            double result = model.runModel(OneHotEncoder.encode(plan));

            return ProbabilisticDoubleInterval.ofExactly(result);
        } catch(Exception e) {
            e.printStackTrace();
            return ProbabilisticDoubleInterval.zero;
        }
    }

    @Override public ProbabilisticDoubleInterval getParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);
            double result = model.runModel(OneHotEncoder.encode(plan));

            return ProbabilisticDoubleInterval.ofExactly(result);
        } catch(Exception e) {
            e.printStackTrace();
            return ProbabilisticDoubleInterval.zero;
        }
    }

    /** Returns a squashed cost estimate. */
    @Override public double getSquashedEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);
            double result = model.runModel(OneHotEncoder.encode(plan));

            return result;
        } catch(Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override public double getSquashedParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        try {
            Configuration config = plan
                .getOptimizationContext()
                .getConfiguration();
            OrtMLModel model = OrtMLModel.getInstance(config);
            double result = model.runModel(OneHotEncoder.encode(plan));

            return result;
        } catch(Exception e) {
            e.printStackTrace();
            return 0;
        }
    }

    @Override public Tuple<List<ProbabilisticDoubleInterval>, List<Double>> getParallelOperatorJunctionAllCostEstimate(PlanImplementation plan, Operator operator) {
        List<ProbabilisticDoubleInterval> intervalList = new ArrayList<ProbabilisticDoubleInterval>();
        List<Double> doubleList = new ArrayList<Double>();
        intervalList.add(this.getEstimate(plan, true));
        doubleList.add(this.getSquashedEstimate(plan, true));

        return new Tuple<>(intervalList, doubleList);
    }

    public PlanImplementation pickBestExecutionPlan(
            Collection<PlanImplementation> executionPlans,
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages) {
        final PlanImplementation bestPlanImplementation = executionPlans.stream()
                .reduce((p1, p2) -> {
                    final double t1 = p1.getSquashedCostEstimate();
                    final double t2 = p2.getSquashedCostEstimate();
                    return t1 < t2 ? p1 : p2;
                })
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));
        return bestPlanImplementation;
    }
}
