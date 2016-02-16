package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.SanityChecker;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimationTraversal;
import org.qcri.rheem.core.optimizer.enumeration.PartialPlan;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerator;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Formats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Describes a job that is to be executed using Rheem.
 */
public class Job {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final AtomicBoolean hasBeenExecuted = new AtomicBoolean(false);

    private final RheemContext rheemContext;

    private final Configuration configuration;

    private final RheemPlan rheemPlan;

    Job(RheemContext rheemContext, RheemPlan rheemPlan) {
        this.rheemContext = rheemContext;
        this.configuration = this.rheemContext.getConfiguration().fork();
        this.rheemPlan = rheemPlan;
    }

    /**
     * Execute this job.
     */
    public void execute() {
        // Make sure that each job is only executed once.
        if (this.hasBeenExecuted.getAndSet(true)) {
            throw new RheemException("Job has already been executed.");
        }

        // Get an execution plan.
        long optimizerStartTime = System.currentTimeMillis();
        ExecutionPlan executionPlan = this.getExecutionPlan();
        long optimizerFinishTime = System.currentTimeMillis();
        this.logger.info("Optimization done in {}.", Formats.formatDuration(optimizerFinishTime - optimizerStartTime));
        this.logger.info("Picked execution plan:\n{}", executionPlan.toExtensiveString());

        // Take care of the execution.
        this.deployAndRun(executionPlan);
    }

    /**
     * Determine a good/the best execution plan from a given {@link RheemPlan}.
     */
    private ExecutionPlan getExecutionPlan() {
        // Apply the mappings to the plan to form a hyperplan.
        this.applyMappingsToRheemPlan();

        // Make the cardinality estimation pass.
        this.estimateCardinalities();
//        this.estimateExecutionTimes();

        // Enumerate plans and pick the best one.
        return this.extractExecutionPlan();
    }

    /**
     * Apply all available transformations in the {@link #configuration} to the {@link RheemPlan}.
     */
    private void applyMappingsToRheemPlan() {
        boolean isAnyChange;
        int epoch = Operator.FIRST_EPOCH;
        final Collection<PlanTransformation> transformations = this.gatherTransformations();
        do {
            epoch++;
            final int numTransformations = this.applyAndCountTransformations(transformations, epoch);
            this.logger.debug("Applied {} transformations in epoch {}.", numTransformations, epoch);
            isAnyChange = numTransformations > 0;
        } while (isAnyChange);

        // Check that the mappings have been applied properly.
        this.checkHyperplanSanity();
    }


    /**
     * Gather all available {@link PlanTransformation}s from the {@link #configuration}.
     */
    private Collection<PlanTransformation> gatherTransformations() {
        return this.configuration.getPlatformProvider().provideAll().stream()
                .flatMap(platform -> platform.getMappings().stream())
                .flatMap(mapping -> mapping.getTransformations().stream())
                .collect(Collectors.toList());
    }

    /**
     * Apply all {@code transformations} to the {@code plan}.
     *
     * @param transformations transformations to apply
     * @param epoch           the new epoch
     * @return the number of applied transformations
     */
    private int applyAndCountTransformations(Collection<PlanTransformation> transformations, int epoch) {
        return transformations.stream()
                .mapToInt(transformation -> transformation.transform(this.rheemPlan, epoch))
                .sum();
    }

    /**
     * Check that the given {@link RheemPlan} is as we expect it to be in the following steps.
     */
    private void checkHyperplanSanity() {
        // We make some assumptions on the hyperplan. Make sure that they hold. After all, the transformations might
        // have bugs.
        final SanityChecker sanityChecker = new SanityChecker(this.rheemPlan);
        if (!sanityChecker.checkAllCriteria()) {
            throw new IllegalStateException("Hyperplan is not in an expected state.");
        }
    }

    /**
     * Go over the given {@link RheemPlan} and estimate the cardinalities of data being passed between its
     * {@link Operator}s.
     */
    private void estimateCardinalities() {
        CardinalityEstimatorManager cardinalityEstimatorManager = new CardinalityEstimatorManager(this.configuration);
        cardinalityEstimatorManager.pushCardinalityEstimation(this.rheemPlan);
//        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates = cardinalityEstimatorManager.getCache();
//        cardinalityEstimates.entrySet().stream().forEach(entry ->
//                this.logger.debug("Cardinality estimate for {}: {}", entry.getKey(), entry.getValue()));
//        return cardinalityEstimates;
    }

    /**
     * Go over the given {@link RheemPlan} and estimate the execution times of its {@link ExecutionOperator}s.
     */
    private Map<ExecutionOperator, TimeEstimate> estimateExecutionTimes(Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        final Map<ExecutionOperator, TimeEstimate> timeEstimates = TimeEstimationTraversal.traverse(
                this.rheemPlan, this.configuration, cardinalityEstimates);
        timeEstimates.entrySet().forEach(entry ->
                this.logger.debug("Time estimate for {}: {}", entry.getKey(), entry.getValue()));
        return timeEstimates;
    }

    /**
     * Enumerate possible execution plans from the given {@link RheemPlan} and determine the (seemingly) best one.
     */
    private ExecutionPlan extractExecutionPlan() {
        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = this.configuration.getTimeEstimateComparatorProvider().provide();

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = new PlanEnumerator(this.rheemPlan, this.configuration);
        this.configuration.getPruningStrategiesProvider().forEach(planEnumerator::addPruningStrategy);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        final Collection<PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        this.logger.info("Enumerated {} plans.", executionPlans.size());
        for (PartialPlan partialPlan : executionPlans) {
            this.logger.debug("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan.
        // Make sure that an execution plan can be created.
        final PartialPlan partialPlan = executionPlans.stream()
                .filter(plan -> plan.getExecutionPlan() != null)
                .reduce((p1, p2) -> {
                    final TimeEstimate t1 = p1.getExecutionPlan().estimateExecutionTime(this.configuration);
                    final TimeEstimate t2 = p2.getExecutionPlan().estimateExecutionTime(this.configuration);
                    return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
                })
                .map(plan -> {
                    this.logger.info("Picked plan's cost estimate is {}.", plan.getExecutionPlan().estimateExecutionTime(this.configuration));
                    return plan;
                })
                .orElseThrow(() -> new IllegalStateException("Could not find an execution plan."));

        return partialPlan.getExecutionPlan().toExecutionPlan();
    }

    /**
     * Dummy implementation: Have the platforms execute the given execution plan.
     */
    private void deployAndRun(ExecutionPlan executionPlan) {
        final CrossPlatformExecutor crossPlatformExecutor = new CrossPlatformExecutor();
        crossPlatformExecutor.execute(executionPlan);
    }

    /**
     * Modify the {@link Configuration} to control the {@link Job} execution.
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }
}
