package org.qcri.rheem.core.api;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.Optimizer;
import org.qcri.rheem.core.optimizer.PlanEnumeration;
import org.qcri.rheem.core.optimizer.PlanEnumerator;
import org.qcri.rheem.core.optimizer.SanityChecker;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;

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

        new SanityChecker(physicalPlan).checkAllCriteria();
        final PlanEnumerator planEnumerator = new PlanEnumerator(physicalPlan);
        planEnumerator.run();
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.getComprehensiveEnumeration();
        logger.info("Enumerated {} plans.", comprehensiveEnumeration.getPartialPlans().size());

        PhysicalPlan executionPlan = this.optimizer.buildExecutionPlan(physicalPlan);

        for (Operator sink : executionPlan.getSinks()) {
            final ExecutionOperator executableSink = (ExecutionOperator) sink;
            final Platform platform = ((ExecutionOperator) sink).getPlatform();
            platform.evaluate(executableSink);
        }
    }

    /**
     * Apply all {@link #transformations} to the {@code plan}.
     * @param physicalPlan the plan to transform
     * @param epoch the new epoch
     * @return the number of applied transformations
     */
    private int applyAndCountTransformations(PhysicalPlan physicalPlan, int epoch) {
        return this.transformations.stream()
                .mapToInt(transformation -> transformation.transform(physicalPlan, epoch))
                .sum();
    }

}
