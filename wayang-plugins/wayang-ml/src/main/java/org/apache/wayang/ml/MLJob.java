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

package org.apache.wayang.ml;

import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.monitor.DisabledMonitor;
import org.apache.wayang.core.monitor.FileMonitor;
import org.apache.wayang.core.monitor.Monitor;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.apache.wayang.core.optimizer.costs.TimeEstimate;
import org.apache.wayang.core.optimizer.costs.TimeToCostConverter;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.optimizer.enumeration.StageAssignmentTraversal;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.PlanMetrics;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.AtomicExecutionGroup;
import org.apache.wayang.core.platform.Breakpoint;
import org.apache.wayang.core.platform.CardinalityBreakpoint;
import org.apache.wayang.core.platform.ConjunctiveBreakpoint;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.platform.ExecutionState;
import org.apache.wayang.core.platform.FixBreakpoint;
import org.apache.wayang.core.platform.NoIterationBreakpoint;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.profiling.CardinalityRepository;
import org.apache.wayang.core.profiling.CostMeasurement;
import org.apache.wayang.core.profiling.ExecutionLog;
import org.apache.wayang.core.profiling.ExecutionPlanMeasurement;
import org.apache.wayang.core.profiling.InstrumentationStrategy;
import org.apache.wayang.core.profiling.PartialExecutionMeasurement;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.OneTimeExecutable;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Describes a job that is to be executed using Wayang.
 */
public class MLJob extends Job {

    MLJob(WayangContext wayangContext, String name, Monitor monitor, WayangPlan wayangPlan, Experiment experiment, String... udfJars) {
        super(wayangContext, name, monitor, wayangPlan, experiment, udfJars);
    }

    @Override
    protected void doExecute() {
        // Make sure that each job is only executed once.
        if (this.hasBeenExecuted.getAndSet(true)) {
            throw new WayangException("Job has already been executed.");
        }

        try {

            // Prepare the #wayangPlan for the optimization.
            this.optimizationRound.start();
            this.prepareWayangPlan();

            // Estimate cardinalities and execution times for the #wayangPlan.
            this.estimateKeyFigures();

            // Get an execution plan.
            int executionId = 0;
            ExecutionPlan executionPlan = this.createInitialExecutionPlan();
            this.optimizationRound.stop();
            if (this.experiment != null) {
                this.experiment.addMeasurement(ExecutionPlanMeasurement.capture(
                        executionPlan,
                        String.format("execution-plan-%d", executionId)
                ));
            }

            // TODO: generate run ID. For now we fix this because we can't handle multiple jobs, neither in montoring nor execution.
            String runId = "1";
            try {
                monitor.initialize(this.configuration, runId, executionPlan.toJsonList());
            } catch (Exception e) {
                this.logger.warn("Failed to initialize monitor: {}", e);
            }


            // Take care of the execution.
            while (!this.execute(executionPlan, executionId)) {
                this.optimizationRound.start();
                if (this.postProcess(executionPlan, executionId)) {
                    executionId++;
                    if (this.experiment != null) {
                        this.experiment.addMeasurement(ExecutionPlanMeasurement.capture(
                                executionPlan,
                                String.format("execution-plan-%d", executionId)
                        ));
                    }
                }
                this.optimizationRound.stop();
            }

            this.stopWatch.start("Post-processing");
            if (this.configuration.getBooleanProperty("wayang.core.log.enabled")) {
                this.logExecution();
            }
        } catch (WayangException e) {
            throw e;
        } catch (Throwable t) {
            throw new WayangException("Job execution failed.", t);
        } finally {
            this.stopWatch.stopAll();
            this.stopWatch.start("Post-processing", "Release Resources");
            this.releaseResources();
            this.stopWatch.stop("Post-processing");
            this.logger.info("StopWatch results:\n{}", this.stopWatch.toPrettyString());
        }
    }

    /**
     * Start executing the given {@link ExecutionPlan} with all bells and whistles, such as instrumentation,
     * logging of the plan, and measuring the execution time.
     *
     * @param executionPlan that should be executed
     * @param executionId   an identifier for the current execution
     * @return whether the execution of the {@link ExecutionPlan} is completed
     */
    @Override protected boolean execute(ExecutionPlan executionPlan, int executionId) {
        final TimeMeasurement currentExecutionRound = this.executionRound.start(String.format("Execution %d", executionId));

        // Ensure existence of the #crossPlatformExecutor.
        if (this.crossPlatformExecutor == null) {
            final InstrumentationStrategy instrumentation = this.configuration.getInstrumentationStrategyProvider().provide();
            this.crossPlatformExecutor = new CrossPlatformExecutor(this, instrumentation);
        }

        if (this.configuration.getOptionalBooleanProperty("wayang.core.debug.skipexecution").orElse(false)) {
            return true;
        }
        if (this.configuration.getBooleanProperty("wayang.core.optimizer.reoptimize")) {
            this.setUpBreakpoint(executionPlan, currentExecutionRound);
        }

        // Log the current executionPlan.
        this.logStages(executionPlan);

        // Trigger the execution.
        currentExecutionRound.start("Execute");
        boolean isExecutionComplete = this.crossPlatformExecutor.executeUntilBreakpoint(
                executionPlan, this.optimizationContext
        );
        executionRound.stop();

        // Return.
        return isExecutionComplete;
    }

    /**
     * Determine a good/the best execution plan from a given {@link WayangPlan}.
     */
    @Override protected ExecutionPlan createInitialExecutionPlan() {
        this.logger.info("Enumerating execution plans...");

        this.optimizationRound.start("Create Initial Execution Plan");

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator();

        final TimeMeasurement enumerateMeasurment = this.optimizationRound.start("Create Initial Execution Plan", "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        this.optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.debug("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.optimizationRound.start("Create Initial Execution Plan", "Pick Best Plan");
        this.pickBestExecutionPlan(executionPlans, null, null, null);
        HashMap<PlanImplementation, ExecutionPlan> planMappings = this.getConfiguration().getCostModel().getPlanMappings();

        if (planMappings.containsKey(this.planImplementation)) {
            final ExecutionPlan executionPlan = planMappings.get(this.planImplementation);

            this.timeEstimates.add(planImplementation.getTimeEstimate());
            this.costEstimates.add(planImplementation.getCostEstimate());
            this.optimizationRound.stop("Create Initial Execution Plan", "Pick Best Plan");

            this.planImplementation.mergeJunctionOptimizationContexts();

            this.planImplementation.logTimeEstimates();

            System.out.println("[CREATE INITIAL]: " + this.planImplementation.getOperators());
            System.out.println("[CREATE INITIAL]: " + executionPlan.toExtensiveString());
            System.out.println("[CREATE INITIAL]: " + executionPlan.getStages());
            System.out.println("[CREATE INITIAL]: " + executionPlan.collectAllTasks());

            assert executionPlan.isSane();

            return executionPlan;
        } else {

            this.timeEstimates.add(planImplementation.getTimeEstimate());
            this.costEstimates.add(planImplementation.getCostEstimate());
            this.optimizationRound.stop("Create Initial Execution Plan", "Pick Best Plan");

            this.logger.info("Compiling execution plan...");
            this.optimizationRound.start("Create Initial Execution Plan", "Split Stages");
            final ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.createFrom(this.planImplementation);
            final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
            this.optimizationRound.stop("Create Initial Execution Plan", "Split Stages");

            this.planImplementation.mergeJunctionOptimizationContexts();

            this.planImplementation.logTimeEstimates();

            System.out.println("[CREATE INITIAL TRADITIONAL]: " + executionPlan.toExtensiveString());

            //assert executionPlan.isSane();
            this.optimizationRound.stop("Create Initial Execution Plan");

            return executionPlan;
        }
    }

    /**
     * Enumerate possible execution plans from the given {@link WayangPlan} and determine the (seemingly) best one.
     */
    @Override protected void updateExecutionPlan(ExecutionPlan executionPlan) {
        // Defines the plan that we want to use in the end.
        // Find and copy the open Channels.
        final Set<ExecutionStage> completedStages = this.crossPlatformExecutor.getCompletedStages();
        final Set<ExecutionTask> completedTasks = completedStages.stream()
                .flatMap(stage -> stage.getAllTasks().stream())
                .collect(Collectors.toSet());

        // Find Channels that have yet to be consumed by unexecuted ExecutionTasks and scrap unexecuted bits of the plan.
        final Set<Channel> openChannels = executionPlan.retain(completedStages);

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = this.createPlanEnumerator(executionPlan, openChannels);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        final Collection<PlanImplementation> executionPlans = comprehensiveEnumeration.getPlanImplementations();
        this.logger.debug("Enumerated {} plans.", executionPlans.size());
        for (PlanImplementation planImplementation : executionPlans) {
            this.logger.debug("Plan with operators: {}", planImplementation.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        this.pickBestExecutionPlan(executionPlans, executionPlan, openChannels, completedStages);
        this.timeEstimates.add(this.planImplementation.getTimeEstimate());
        this.costEstimates.add(this.planImplementation.getCostEstimate());
        HashMap<PlanImplementation, ExecutionPlan> planMappings = this.getConfiguration().getCostModel().getPlanMappings();
        System.out.println("[UPDATED]: " + planMappings);

        if (!planMappings.isEmpty()) {

            executionPlan = planMappings.get(this.planImplementation);
            //executionPlan.expand(planMappings.get(this.planImplementation));
            System.out.println("[UPDATED]: " + executionPlan.toExtensiveString());

            this.planImplementation.mergeJunctionOptimizationContexts();

        } else {
            ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.recreateFrom(
                    planImplementation, executionPlan, openChannels, completedStages
            );
            final ExecutionPlan executionPlanExpansion = ExecutionPlan.createFrom(executionTaskFlow, this.stageSplittingCriterion);
            executionPlan.expand(executionPlanExpansion);

            this.planImplementation.mergeJunctionOptimizationContexts();
        }

        assert executionPlan.isSane();
    }

    /**
     * Injects the cardinalities obtained from {@link Channel} instrumentation, potentially updates the {@link ExecutionPlan}
     * through re-optimization, and collects measured data.
     *
     * @return whether the {@link ExecutionPlan} has been re-optimized
     */
    protected boolean postProcess(ExecutionPlan executionPlan, int executionId) {
        if (this.crossPlatformExecutor.isVetoingPlanChanges()) {
            this.logger.info("The cross-platform executor is currently not allowing re-optimization.");
            return false;
        }

        final TimeMeasurement round = this.optimizationRound.start(String.format("Post-processing %d", executionId));

        round.start("Reestimate Cardinalities&Time");
        boolean isCardinalitiesUpdated = this.reestimateCardinalities(this.crossPlatformExecutor);
        round.stop("Reestimate Cardinalities&Time");

        round.start("Update Execution Plan");
        if (isCardinalitiesUpdated) {
            this.logger.info("Re-optimizing execution plan.");
            this.updateExecutionPlan(executionPlan);
        } else {
            this.logger.info("Skipping re-optimization: no new insights on cardinalities.");
            this.timeEstimates.add(this.timeEstimates.get(this.timeEstimates.size() - 1));
            this.costEstimates.add(this.costEstimates.get(this.costEstimates.size() - 1));

        }
        round.stop("Update Execution Plan");

        round.stop();

        return true;
    }
}
