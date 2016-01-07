package org.qcri.rheem.core.api;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.plan.Sink;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;
import java.util.LinkedList;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    /**
     * All registered mappings.
     */
    private final Collection<PlanTransformation> transformations = new LinkedList<>();

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
     * @param platform the {@link Platform} to register
     */
    public void register(Platform platform) {
        // TODO
    }

    /**
     * Execute a plan.
     * @param physicalPlan the plan to execute
     */
    public void execute(PhysicalPlan physicalPlan) {
        // NB: This is a dummy implementation to make the simplest case work.
        for (PlanTransformation transformation : this.transformations) {
            transformation.transform(physicalPlan);
        }

        for (Sink sink : physicalPlan.getSinks()) {
            final ExecutionOperator executableSink = (ExecutionOperator) sink;
            final Platform platform = ((ExecutionOperator) sink).getPlatform();
            platform.evaluate(executableSink);
        }
    }

}
