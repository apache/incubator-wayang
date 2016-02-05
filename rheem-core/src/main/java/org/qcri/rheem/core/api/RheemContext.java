package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.configuration.ConfigurationProvider;
import org.qcri.rheem.core.api.configuration.FunctionalConfigurationProvider;
import org.qcri.rheem.core.api.configuration.MapBasedConfigurationProvider;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FallbackCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.platform.Platform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.LinkedList;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public static final String BASIC_PLUGIN_ACTIVATOR = "org.qcri.rheem.basic.plugin.Activator";

    private final Configuration configuration = bootstrapConfiguration();

    private Configuration bootstrapConfiguration() {
        Configuration configuration = new Configuration(this);
        bootstrapCardinalityEstimationProvider(configuration);
        bootstrapSelectivityProviders(configuration);
        return configuration;
    }

    private void bootstrapCardinalityEstimationProvider(final Configuration configuration) {
        // Safety net: provide a fallback estimator.
        ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> fallbackProvider =
                new FunctionalConfigurationProvider<OutputSlot<?>, CardinalityEstimator>(
                        outputSlot -> new FallbackCardinalityEstimator()
                ).withSlf4jWarning("Creating fallback estimator for {}.");
        
        // Default option: Implementations define their estimators.
        ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> defaultProvider =
                new FunctionalConfigurationProvider<>(fallbackProvider, (outputSlot, requestee) ->
                        outputSlot.getOwner()
                                .getCardinalityEstimator(outputSlot.getIndex(), configuration)
                                .orElse(null)
                );
        
        // Customizable layer: Users can override manually.
        ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> overrideProvider =
                new MapBasedConfigurationProvider<>(defaultProvider);

        configuration.setCardinalityEstimatorProvider(overrideProvider);
    }
    
    private void bootstrapSelectivityProviders(Configuration configuration) {
        {
            // Safety net: provide a fallback selectivity.
            ConfigurationProvider<Class<? extends Predicate>, Double> fallbackProvider =
                    new FunctionalConfigurationProvider<Class<? extends Predicate>, Double>(
                            predicateClass -> 0.5d
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            ConfigurationProvider<Class<? extends Predicate>, Double> overrideProvider =
                    new MapBasedConfigurationProvider<>(fallbackProvider);

            configuration.setPredicateSelectivityProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback selectivity.
            ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> fallbackProvider =
                    new FunctionalConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double>(
                            predicateClass -> 1d
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> overrideProvider =
                    new MapBasedConfigurationProvider<>(fallbackProvider);

            configuration.setMultimapSelectivityProvider(overrideProvider);
        }
    }

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
