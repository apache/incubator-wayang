package org.qcri.rheem.core.api;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.Optimizer;
import org.qcri.rheem.core.optimizer.PlanEnumeration;
import org.qcri.rheem.core.optimizer.PlanEnumerator;
import org.qcri.rheem.core.optimizer.SanityChecker;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimatorManager;
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
import java.util.LinkedList;
import java.util.Map;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * All registered mappings.
     */
    private final Collection<PlanTransformation> transformations = new LinkedList<>();

    private final Optimizer optimizer = new Optimizer();

    private final CardinalityEstimatorManager cardinalityEstimatorManager = new CardinalityEstimatorManager(this);

    public RheemContext() {
        final String activateClassName = "org.qcri.rheem.basic.plugin.Activator";
        activatePackage(activateClassName);
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
        // NB: This is a dummy implementation to make the simplest case work.

        // TODO: introduce calls only, for example the following block of code should be a simple call to the optimizer

        boolean isAnyChange;
        int epoch = Operator.FIRST_EPOCH;
        do {
            epoch++;
            final int numTransformations = applyAndCountTransformations(physicalPlan, epoch);
            logger.info("Applied {} transformations in epoch {}.", numTransformations, epoch);
            isAnyChange = numTransformations > 0;
        } while (isAnyChange);

        // We make some assumptions on the hyperplan. Make sure that they hold. After all, the transformations might
        // have bugs.
        new SanityChecker(physicalPlan).checkAllCriteria();

        // Make the cardinality estimation pass.
        final Map<OutputSlot<?>, CardinalityEstimate> cardinalityEstimates =
                this.getCardinalityEstimatorManager().estimateAllCardinatilities(physicalPlan);
        cardinalityEstimates.entrySet().stream().forEach(entry ->
                this.logger.info("Cardinality estimate for {}: {}", entry.getKey(), entry.getValue()));

        // Enumerate all possible plan. TODO: Prune them (using the cardinality estimates, amongst others).
        final PlanEnumerator planEnumerator = new PlanEnumerator(physicalPlan);
        planEnumerator.run();
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.getComprehensiveEnumeration();
        final Collection<PlanEnumeration.PartialPlan> executionPlans = comprehensiveEnumeration.getPartialPlans();
        logger.info("Enumerated {} plans.", executionPlans.size());
        for (PlanEnumeration.PartialPlan partialPlan : executionPlans) {
            logger.info("Plan with operators: {}", partialPlan.getOperators());
        }

        // Pick an execution plan. TODO: Pick the best one.
        PhysicalPlan executionPlan = executionPlans.stream()
                .findAny().orElseThrow(IllegalStateException::new)
                .toPhysicalPlan();

        // Take care of the execution.
        deployAndRun(executionPlan);
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
