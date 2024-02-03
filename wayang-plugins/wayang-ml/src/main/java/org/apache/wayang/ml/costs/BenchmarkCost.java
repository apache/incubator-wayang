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

import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.costs.DefaultEstimatableCost;
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

import java.util.Collection;
import java.util.Set;
import java.util.List;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class BenchmarkCost extends DefaultEstimatableCost  {
    public EstimatableCostFactory getFactory() {
        return new Factory();
    }

    public static class Factory implements EstimatableCostFactory {
        public EstimatableCost makeCost() {
            return new BenchmarkCost();
        }
    }

    public PlanImplementation pickBestExecutionPlan(
            Collection<PlanImplementation> executionPlans,
            ExecutionPlan existingPlan,
            Set<Channel> openChannels,
            Set<ExecutionStage> executedStages) {
        // Measure time needed for a decision: picking the better plan.
        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("/var/www/html/data/decisions.txt", true));
            final Experiment experiment = new Experiment("wayang-ml", new Subject("Wayang", "0.1"));
            final StopWatch stopWatch = new StopWatch(experiment);
            final TimeMeasurement decisionRound = stopWatch.getOrCreateRound("Decision");
            final TimeMeasurement currentDecisionRound = decisionRound.start(String.format("Decision %d", 1));
            final PlanImplementation bestPlanImplementation = executionPlans.stream()
                    .reduce((p1, p2) -> {
                        final double t1 = p1.getSquashedCostEstimate();
                        final double t2 = p2.getSquashedCostEstimate();
                        return t1 < t2 ? p1 : p2;
                    })
                    .orElseThrow(() -> new WayangException("Could not find an execution plan."));
            decisionRound.stop();
            writer.write(String.format("Decision: %s",currentDecisionRound.getMillis()));
            writer.newLine();
            writer.close();

            return bestPlanImplementation;
        } catch (Exception e) {
            System.out.println("Couldnt write to File error: " + e);
        }

        return null;
    }
}
