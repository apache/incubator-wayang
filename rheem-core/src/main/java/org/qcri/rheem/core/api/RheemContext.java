package org.qcri.rheem.core.api;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.enumeration.InternalOperatorPruningStrategy;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumeration;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerator;
import org.qcri.rheem.core.optimizer.SanityChecker;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.ResourceUsageToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimationTraversal;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.Map;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final String BASIC_PLUGIN_ACTIVATOR = "org.qcri.rheem.basic.plugin.Activator";

    /**
     * All registered mappings.
     */
    private final Collection<PlanTransformation> transformations = new LinkedList<>();

    private final CardinalityEstimatorManager cardinalityEstimatorManager = new CardinalityEstimatorManager(this);

    public RheemContext() {
        activatePackage(BASIC_PLUGIN_ACTIVATOR);
    }

    /**
     * This function activates a Rheem package on this Rheem context. For that purpose, the package must provide an
     * activator class with the static method {@code activate(RheemContext)} that registers all resources of that
     * package with the given Rheem context.
     *
     * @param activatorClassName the fully qualified name of the above described activator class
     */
    public void activatePackage(String activatorClassName) {
        try {
            final Class<?> activatorClass = Class.forName(activatorClassName);
            final Method activateMethod = activatorClass.getMethod("activate", RheemContext.class);
            activateMethod.invoke(null, this);
        } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Could not activate Rheem package.", e);
        }
    }

    /**
     * Register a mapping that Rheem will then consider when translating Rheem plans into executable plans.
     *
     * @param transformation a {@link PlanTransformation} that describes the operator mapping
     */
    public void register(PlanTransformation transformation) {
        this.transformations.add(transformation);
    }

    /**
     * Register a mapping that Rheem will then consider when translating Rheem plans into executable plans.
     *
     * @param mapping a {@link Mapping} that comprises a collection of {@link PlanTransformation}s
     */
    public void register(Mapping mapping) {
        for (PlanTransformation planTransformation : mapping.getTransformations()) {
            register(planTransformation);
        }
    }

    /**
     * Register a platform that Rheem will then use for execution.
     *
     * @param platform the {@link Platform} to register
     */
    public void register(Platform platform) {
        // TODO
    }

    /**
     * Execute a plan.
     *
     * @param physicalPlan the plan to execute
     */
    public void execute(PhysicalPlan physicalPlan) {
        PhysicalPlan executionPlan = getExecutionPlan(physicalPlan);

        // Take care of the execution.
        deployAndRun(executionPlan);
    }

    /**
     * Determine a good/the best execution plan from a given {@link PhysicalPlan}.
     */
    private PhysicalPlan getExecutionPlan(PhysicalPlan physicalPlan) {
        // Apply the mappings to the plan to form a hyperplan.
        applyMappings(physicalPlan);

        // Make the cardinality estimation pass.
        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates = estimateCardinalities(physicalPlan);

        final Map<ExecutionOperator, TimeEstimate> timeEstimates = estimateExecutionTimes(physicalPlan, cardinalityEstimates);

        // Enumerate plans and pick the best one.
        final PhysicalPlan pickedExecutionPlan = extractExecutionPlan(physicalPlan, timeEstimates);
        return pickedExecutionPlan;
    }

    /**
     * Apply all available {@link #transformations} to the {@link PhysicalPlan}.
     */
    private void applyMappings(PhysicalPlan physicalPlan) {
        boolean isAnyChange;
        int epoch = Operator.FIRST_EPOCH;
        do {
            epoch++;
            final int numTransformations = applyAndCountTransformations(physicalPlan, epoch);
            logger.info("Applied {} transformations in epoch {}.", numTransformations, epoch);
            isAnyChange = numTransformations > 0;
        } while (isAnyChange);

        // Check that the mappings have been applied properly.
        checkHyperplanSanity(physicalPlan);
    }

    /**
     * Check that the given {@link PhysicalPlan} is as we expect it to be in the following steps.
     */
    private void checkHyperplanSanity(PhysicalPlan physicalPlan) {
        // We make some assumptions on the hyperplan. Make sure that they hold. After all, the transformations might
        // have bugs.
        final SanityChecker sanityChecker = new SanityChecker(physicalPlan);
        if (!sanityChecker.checkAllCriteria()) {
            throw new IllegalStateException("Hyperplan is not in an expected state.");
        }
    }

    /**
     * Go over the given {@link PhysicalPlan} and estimate the cardinalities of data being passed between its
     * {@link Operator}s.
     */
    private Map<OutputSlot<?>, CardinalityEstimate> estimateCardinalities(PhysicalPlan physicalPlan) {
        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates =
                this.getCardinalityEstimatorManager().estimateAllCardinatilities(physicalPlan);
        cardinalityEstimates.entrySet().stream().forEach(entry ->
                this.logger.debug("Cardinality estimate for {}: {}", entry.getKey(), entry.getValue()));
        return cardinalityEstimates;
    }

    /**
     * Go over the given {@link PhysicalPlan} and estimate the execution times of its
     * {@link ExecutionOperator}s.
     */
    private Map<ExecutionOperator, TimeEstimate> estimateExecutionTimes(PhysicalPlan physicalPlan, Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates) {
        ResourceUsageProfileToTimeConverter timeConverter = ResourceUsageProfileToTimeConverter.createDefault(
                ResourceUsageToTimeConverter.createLinearCoverter(0.001d),
                ResourceUsageToTimeConverter.createLinearCoverter(0.01d),
                ResourceUsageToTimeConverter.createLinearCoverter(0.01d),
                (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
        );
        final Map<ExecutionOperator, TimeEstimate> timeEstimates = TimeEstimationTraversal.traverse(physicalPlan,
                timeConverter,
                cardinalityEstimates);
        timeEstimates.entrySet().forEach(entry ->
                this.logger.info("Time estimate for {}: {}", entry.getKey(), entry.getValue()));
        return timeEstimates;
    }

    /**
     * Enumerate possible execution plans from the given {@link PhysicalPlan} and determine the (seemingly) best one.
     */
    private PhysicalPlan extractExecutionPlan(final PhysicalPlan physicalPlan,
                                              final Map<ExecutionOperator, TimeEstimate> timeEstimates) {

        // Defines the plan that we want to use in the end.
        final Comparator<TimeEstimate> timeEstimateComparator = TimeEstimate.expectionValueComparator();

        // Enumerate all possible plan.
        final PlanEnumerator planEnumerator = new PlanEnumerator(physicalPlan, timeEstimates);
        planEnumerator.addPruningStrategy(new InternalOperatorPruningStrategy(
                timeEstimates,
                timeEstimateComparator));
        planEnumerator.run();

        final PlanEnumeration comprehensiveEnumeration = planEnumerator.getComprehensiveEnumeration();
        final Collection<PlanEnumeration.PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        logger.info("Enumerated {} plans.", executionPlans.size());
        for (PlanEnumeration.PartialPlan partialPlan : executionPlans) {
            logger.info("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan.
        return executionPlans.stream()
                .reduce((p1, p2) -> {
                    final TimeEstimate t1 = p1.getExecutionTimeEstimate(timeEstimates);
                    final TimeEstimate t2 = p2.getExecutionTimeEstimate(timeEstimates);
                    return timeEstimateComparator.compare(t1, t2) > 0 ? p1 : p2;
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
     * Apply all {@link #transformations} to the {@code plan}.
     *
     * @param physicalPlan the plan to transform
     * @param epoch        the new epoch
     * @return the number of applied transformations
     */
    private int applyAndCountTransformations(PhysicalPlan physicalPlan, int epoch) {
        return this.transformations.stream()
                .mapToInt(transformation -> transformation.transform(physicalPlan, epoch))
                .sum();
    }

    public CardinalityEstimatorManager getCardinalityEstimatorManager() {
        return cardinalityEstimatorManager;
    }
}
