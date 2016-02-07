package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.SanityChecker;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimationTraversal;
import org.qcri.rheem.core.optimizer.enumeration.InternalOperatorPruningStrategy;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerator;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.platform.Platform;
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

    private final PhysicalPlan rheemPlan;

    Job(RheemContext rheemContext, PhysicalPlan rheemPlan) {
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
        PhysicalPlan executionPlan = getExecutionPlan();

        // Take care of the execution.
        deployAndRun(executionPlan);
    }

    /**
     * Determine a good/the best execution plan from a given {@link PhysicalPlan}.
     */
    private PhysicalPlan getExecutionPlan() {
        // Apply the mappings to the plan to form a hyperplan.
        applyMappingsToRheemPlan();

        // Make the cardinality estimation pass.
        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates = estimateCardinalities();

        final Map<ExecutionOperator, TimeEstimate> timeEstimates = estimateExecutionTimes(cardinalityEstimates);

        // Enumerate plans and pick the best one.
        final PhysicalPlan pickedExecutionPlan = extractExecutionPlan(timeEstimates);
        return pickedExecutionPlan;
    }

    /**
     * Apply all available transformations in the {@link #configuration} to the {@link PhysicalPlan}.
     */
    private void applyMappingsToRheemPlan() {
        boolean isAnyChange;
        int epoch = Operator.FIRST_EPOCH;
        final Collection<PlanTransformation> transformations = this.gatherMappings();
        do {
            epoch++;
            final int numTransformations = applyAndCountTransformations(transformations, epoch);
            logger.debug("Applied {} transformations in epoch {}.", numTransformations, epoch);
            isAnyChange = numTransformations > 0;
        } while (isAnyChange);

        // Check that the mappings have been applied properly.
        checkHyperplanSanity();
    }


    /**
     * Gather all available {@link PlanTransformation}s from the {@link #configuration}.
     */
    private Collection<PlanTransformation> gatherMappings() {
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
     * Check that the given {@link PhysicalPlan} is as we expect it to be in the following steps.
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
     * Go over the given {@link PhysicalPlan} and estimate the cardinalities of data being passed between its
     * {@link Operator}s.
     */
    private Map<OutputSlot<?>, CardinalityEstimate> estimateCardinalities() {
        CardinalityEstimatorManager cardinalityEstimatorManager = new CardinalityEstimatorManager(this.configuration);
        cardinalityEstimatorManager.pushCardinalityEstimation(this.rheemPlan);
        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates = cardinalityEstimatorManager.getCache();
        cardinalityEstimates.entrySet().stream().forEach(entry ->
                this.logger.debug("Cardinality estimate for {}: {}", entry.getKey(), entry.getValue()));
        return cardinalityEstimates;
    }

    /**
     * Go over the given {@link PhysicalPlan} and estimate the execution times of its
     * {@link ExecutionOperator}s.
     */
    private Map<ExecutionOperator, TimeEstimate> estimateExecutionTimes(Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        final Map<ExecutionOperator, TimeEstimate> timeEstimates = TimeEstimationTraversal.traverse(this.rheemPlan,
                this.configuration,
                cardinalityEstimates);
        timeEstimates.entrySet().forEach(entry ->
                this.logger.debug("Time estimate for {}: {}", entry.getKey(), entry.getValue()));
        return timeEstimates;
    }

    /**
     * Enumerate possible execution plans from the given {@link PhysicalPlan} and determine the (seemingly) best one.
     */
    private PhysicalPlan extractExecutionPlan(final Map<ExecutionOperator, TimeEstimate> timeEstimates) {

        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = TimeEstimate.expectionValueComparator();

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = new PlanEnumerator(this.rheemPlan, timeEstimates);
        planEnumerator.addPruningStrategy(new InternalOperatorPruningStrategy(
                timeEstimates,
                timeEstimateComparator));
        planEnumerator.run();

        final PlanEnumeration comprehensiveEnumeration = planEnumerator.getComprehensiveEnumeration();
        final Collection<PlanEnumeration.PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        logger.info("Enumerated {} plans.", executionPlans.size());
        for (PlanEnumeration.PartialPlan partialPlan : executionPlans) {
            logger.debug("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan.
        return executionPlans.stream()
                .reduce((p1, p2) -> {
                    final TimeEstimate t1 = p1.getExecutionTimeEstimate(timeEstimates);
                    final TimeEstimate t2 = p2.getExecutionTimeEstimate(timeEstimates);
                    return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
                })
                .map(plan -> {
                    this.logger.info("Picked plan's cost estimate is {}.", plan.getExecutionTimeEstimate(timeEstimates));
                    return plan;
                })
                .orElseThrow(IllegalStateException::new)
                .toPhysicalPlan();
    }

    /**
     * Dummy implementation: Have the platforms execute the given execution plan.
     */
    private void deployAndRun(PhysicalPlan executionPlan) {
        for (Operator sink : executionPlan.getSinks()) {
            final ExecutionOperator executableSink = (ExecutionOperator) sink;
            final Platform platform = ((ExecutionOperator) sink).getPlatform();
            platform.evaluate(executableSink);
        }
    }

    /**
     * Modify the {@link Configuration} to control the {@link Job} execution.
     */
    public Configuration getConfiguration() {
        return this.configuration;
    }
}
