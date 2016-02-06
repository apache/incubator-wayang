package org.qcri.rheem.core.api;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
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

    public static final String BASIC_PLUGIN_ACTIVATOR = "org.qcri.rheem.basic.plugin.Activator";

    private final Configuration configuration = Configuration.createDefaultConfiguration(this);

    /**
     * All registered mappings.
     */
    final Collection<PlanTransformation> transformations = new LinkedList<>();



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
        Job job = new Job(this, physicalPlan);
        job.execute();

    }

    public Configuration getConfiguration() {
        return configuration;
    }
}
