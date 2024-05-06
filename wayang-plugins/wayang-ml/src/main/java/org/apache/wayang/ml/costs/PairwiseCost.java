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
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.optimizer.enumeration.StageAssignmentTraversal;
import org.apache.wayang.ml.OrtMLModel;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;

public class PairwiseCost implements EstimatableCost {

    /**
     * <i>Currently not used.</i>
     */
    protected final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
            (producerTask, channel, consumerTask) -> false;

    private ExecutorService executor = Executors.newFixedThreadPool(1);

    public EstimatableCostFactory getFactory() {
        return new Factory();
    }

    public static class Factory implements EstimatableCostFactory {
        @Override public EstimatableCost makeCost() {
            return new PairwiseCost();
        }
    }

    @Override public ProbabilisticDoubleInterval getEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        return ProbabilisticDoubleInterval.zero;
    }

    @Override public ProbabilisticDoubleInterval getParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        return ProbabilisticDoubleInterval.zero;
    }

    /** Returns a squashed cost estimate. */
    @Override public double getSquashedEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        return 0;
    }

    @Override public double getSquashedParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded) {
        return 0;
    }

    private ExecutionPlan createExecutionPlan(PlanImplementation plan) {
        ExecutionTaskFlow etfOne = ExecutionTaskFlow.createFrom(plan);
        return ExecutionPlan.createFrom(etfOne, this.stageSplittingCriterion);
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
                    try {
                        Configuration config = p1
                            .getOptimizationContext()
                            .getConfiguration();
                        OrtMLModel model = OrtMLModel.getInstance(config);

                        TreeNode encodedOne = TreeEncoder.encode(p1);
                        TreeNode encodedTwo = TreeEncoder.encode(p2);

                        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple1 = OrtTensorEncoder.encode(encodedOne);
                        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple2 = OrtTensorEncoder.encode(encodedTwo);

                        int result = model.runPairwise(tuple1, tuple2);
                        System.out.println("[ML] result: " + result);
                        if (result == 1) {
                            System.out.println("PICKED PLAN 2");
                            return p2;
                        }

                        System.out.println("PICKED PLAN 1");
                        return p1;
                    } catch(Exception e) {
                        e.printStackTrace();
                        return p1;
                    }
                })
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));
        return bestPlanImplementation;
    }
}
