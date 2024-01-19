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

import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.util.Tuple;

import java.util.Collection;
import java.util.Set;
import java.util.List;

/**
 * EstimatableCost defines an interface for optimizer cost in terms of
 * methods to estimate a PlanImplementation's cost.
 */
public interface EstimatableCost {
    /* Factory that has to be provided in order to make instances of Costs*/
    public EstimatableCostFactory getFactory();

    public PlanImplementation pickBestExecutionPlan(
            Collection<PlanImplementation> executionPlans,
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages);

    public ProbabilisticDoubleInterval getEstimate(PlanImplementation plan, boolean isOverheadIncluded);

    public ProbabilisticDoubleInterval getParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded);

    /** Returns a squashed cost estimate. */
    public double getSquashedEstimate(PlanImplementation plan, boolean isOverheadIncluded);

    public double getSquashedParallelEstimate(PlanImplementation plan, boolean isOverheadIncluded);

    public Tuple<List<ProbabilisticDoubleInterval>, List<Double>> getParallelOperatorJunctionAllCostEstimate(PlanImplementation plan, Operator operator);
}
