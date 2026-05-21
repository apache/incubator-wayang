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
import org.apache.wayang.core.optimizer.costs.DefaultEstimatableCost;
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

public class PointwiseCost extends DefaultEstimatableCost {

    public static class Factory implements EstimatableCostFactory {
        @Override public EstimatableCost makeCost() {
            return new PointwiseCost();
        }
    }

    @Override
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

                        /*
                        System.out.println(p1.getUtilizedPlatforms());
                        System.out.println(p2.getUtilizedPlatforms());
                        */

                        TreeNode encodedOne = TreeEncoder.encode(p1);
                        TreeNode encodedTwo = TreeEncoder.encode(p2);

                        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple1 = OrtTensorEncoder.encode(encodedOne);
                        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> tuple2 = OrtTensorEncoder.encode(encodedTwo);

                        final double leftCost = Math.exp(model.runModel(tuple1)) - 1;
                        final double rightCost = Math.exp(model.runModel(tuple2)) - 1;

                        /*
                        System.out.println("[ML] left cost: " + leftCost);
                        System.out.println("[ML] right cost: " + rightCost);
                        */

                        return leftCost < rightCost ? p1 : p2;
                    } catch(Exception e) {
                        e.printStackTrace();
                        return p1;
                    }
                })
                .orElseThrow(() -> new WayangException("Could not find an execution plan."));

        Configuration config = bestPlanImplementation
            .getOptimizationContext()
            .getConfiguration();

        if (config.getBooleanProperty("wayang.ml.experience.enabled")) {
            TreeNode encodedPlan = TreeEncoder.encode(bestPlanImplementation);
            config.setProperty(
                "wayang.ml.experience.with-platforms",
                encodedPlan.toString()
            );
        }

        return bestPlanImplementation;
    }
}
