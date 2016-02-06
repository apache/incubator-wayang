package org.qcri.rheem.core.api;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.configuration.ConfigurationProvider;
import org.qcri.rheem.core.api.configuration.ConstantProvider;
import org.qcri.rheem.core.api.configuration.FunctionalConfigurationProvider;
import org.qcri.rheem.core.api.configuration.MapBasedConfigurationProvider;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FallbackCardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.*;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Describes both the configuration of a {@link RheemContext} and {@link Job}s.
 */
public class Configuration {

    private final RheemContext rheemContext;

    private final Configuration parent;

    private ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider;

    private ConfigurationProvider<Class<? extends Predicate>, Double> predicateSelectivityProvider;

    private ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> multimapSelectivityProvider;

    private ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider;

    private ConfigurationProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider;

    private ConstantProvider<LoadProfileToTimeConverter> loadProfileToTimeConverterProvider;

    /**
     * Creates a new top-level instance.
     */
    public Configuration(RheemContext rheemContext) {
        this(rheemContext, null);
    }

    /**
     * Creates a new instance as child of another instance.
     */
    private Configuration(Configuration parent) {
        this(parent.rheemContext, parent);
    }

    /**
     * Basic constructor.
     */
    private Configuration(RheemContext rheemContext, Configuration parent) {
        Validate.notNull(rheemContext);
        this.rheemContext = rheemContext;
        this.parent = parent;

        if (this.parent != null) {
            // Providers for cardinality estimation.
            this.cardinalityEstimatorProvider =
                    new MapBasedConfigurationProvider<>(this.parent.cardinalityEstimatorProvider);
            this.predicateSelectivityProvider =
                    new MapBasedConfigurationProvider<>(this.parent.predicateSelectivityProvider);
            this.multimapSelectivityProvider =
                    new MapBasedConfigurationProvider<>(this.parent.multimapSelectivityProvider);

            // Providers for cost functions.
            this.operatorLoadProfileEstimatorProvider =
                    new MapBasedConfigurationProvider<>(this.parent.operatorLoadProfileEstimatorProvider);
            this.functionLoadProfileEstimatorProvider =
                    new MapBasedConfigurationProvider<>(this.parent.functionLoadProfileEstimatorProvider);
            this.loadProfileToTimeConverterProvider =
                    new ConstantProvider<>(this.parent.loadProfileToTimeConverterProvider);
        }
    }

    public static Configuration createDefaultConfiguration(RheemContext rheemContext) {
        Configuration configuration = new Configuration(rheemContext);
        bootstrapCardinalityEstimationProvider(configuration);
        bootstrapSelectivityProviders(configuration);
        bootstrapLoadAndTimeEstimatorProviders(configuration);
        return configuration;
    }

    private static void bootstrapCardinalityEstimationProvider(final Configuration configuration) {
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

    private static void bootstrapSelectivityProviders(Configuration configuration) {
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

    private static void bootstrapLoadAndTimeEstimatorProviders(Configuration configuration) {
        {
            // Safety net: provide a fallback selectivity.
            ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> fallbackProvider =
                    new FunctionalConfigurationProvider<ExecutionOperator, LoadProfileEstimator>(
                            operator -> new NestableLoadProfileEstimator(
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000)
                            )
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> overrideProvider =
                    new MapBasedConfigurationProvider<>(fallbackProvider);

            configuration.setOperatorLoadProfileEstimatorProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback selectivity.
            ConfigurationProvider<FunctionDescriptor, LoadProfileEstimator> fallbackProvider =
                    new FunctionalConfigurationProvider<FunctionDescriptor, LoadProfileEstimator>(
                            operator -> new NestableLoadProfileEstimator(
                                    DefaultLoadEstimator.createIOLinearEstimator(10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(10000),
                                    null, null
                            )
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            ConfigurationProvider<FunctionDescriptor, LoadProfileEstimator> overrideProvider =
                    new MapBasedConfigurationProvider<>(fallbackProvider);

            configuration.setFunctionLoadProfileEstimatorProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback converter.
            ConstantProvider<LoadProfileToTimeConverter> fallbackProvider =
                    new ConstantProvider<>(LoadProfileToTimeConverter.createDefault(
                            LoadToTimeConverter.createLinearCoverter(0.001d),
                            LoadToTimeConverter.createLinearCoverter(0.001d),
                            LoadToTimeConverter.createLinearCoverter(0.01d),
                            (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
                    )).withSlf4jWarning("Using fallback load-profile-to-time converter.");

            // Add provider to customize behavior on RheemContext level.
            ConstantProvider<LoadProfileToTimeConverter> overrideProvider = new ConstantProvider<>(fallbackProvider);

            configuration.setLoadProfileToTimeConverterProvider(overrideProvider);
        }
    }


    /**
     * Creates a child instance.
     */
    public Configuration fork() {
        return new Configuration(this);
    }

    public RheemContext getRheemContext() {
        return rheemContext;
    }

    public ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> getCardinalityEstimatorProvider() {
        return cardinalityEstimatorProvider;
    }

    public void setCardinalityEstimatorProvider(
            ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider) {
        this.cardinalityEstimatorProvider = cardinalityEstimatorProvider;
    }

    public ConfigurationProvider<Class<? extends Predicate>, Double> getPredicateSelectivityProvider() {
        return predicateSelectivityProvider;
    }

    public void setPredicateSelectivityProvider(
            ConfigurationProvider<Class<? extends Predicate>, Double> predicateSelectivityProvider) {
        this.predicateSelectivityProvider = predicateSelectivityProvider;
    }

    public ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> getMultimapSelectivityProvider() {
        return multimapSelectivityProvider;
    }

    public void setMultimapSelectivityProvider(
            ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> multimapSelectivityProvider) {
        this.multimapSelectivityProvider = multimapSelectivityProvider;
    }

    public ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> getOperatorLoadProfileEstimatorProvider() {
        return operatorLoadProfileEstimatorProvider;
    }

    public void setOperatorLoadProfileEstimatorProvider(ConfigurationProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider) {
        this.operatorLoadProfileEstimatorProvider = operatorLoadProfileEstimatorProvider;
    }

    public ConstantProvider<LoadProfileToTimeConverter> getLoadProfileToTimeConverterProvider() {
        return loadProfileToTimeConverterProvider;
    }

    public void setLoadProfileToTimeConverterProvider(ConstantProvider<LoadProfileToTimeConverter> loadProfileToTimeConverterProvider) {
        this.loadProfileToTimeConverterProvider = loadProfileToTimeConverterProvider;
    }

    public ConfigurationProvider<FunctionDescriptor, LoadProfileEstimator> getFunctionLoadProfileEstimatorProvider() {
        return functionLoadProfileEstimatorProvider;
    }

    public void setFunctionLoadProfileEstimatorProvider(ConfigurationProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider) {
        this.functionLoadProfileEstimatorProvider = functionLoadProfileEstimatorProvider;
    }
}
