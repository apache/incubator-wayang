package org.qcri.rheem.core.api;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.configuration.*;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FallbackCardinalityEstimator;
import org.qcri.rheem.core.optimizer.costs.*;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.profiling.OutboundInstrumentationStrategy;

import java.util.Comparator;

/**
 * Describes both the configuration of a {@link RheemContext} and {@link Job}s.
 */
public class Configuration {

    private static final String BASIC_PLATFORM = "org.qcri.rheem.basic.plugin.RheemBasicPlatform";

    private final RheemContext rheemContext;

    private final Configuration parent;

    private KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider;

    private KeyValueProvider<PredicateDescriptor, Double> predicateSelectivityProvider;

    private KeyValueProvider<FlatMapDescriptor<?, ?>, Double> multimapSelectivityProvider;

    private KeyValueProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider;

    private KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider;

    private ConstantProvider<LoadProfileToTimeConverter> loadProfileToTimeConverterProvider;

    private CollectionProvider<Platform> platformProvider;

    private ConstantProvider<Comparator<TimeEstimate>> timeEstimateComparatorProvider;

    private CollectionProvider<PlanEnumerationPruningStrategy> pruningStrategiesProvider;

    private ConstantProvider<InstrumentationStrategy> instrumentationStrategyProvider;

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
            // Providers for platforms.
            this.platformProvider = new CollectionProvider<>(this.parent.platformProvider);

            // Providers for cardinality estimation.
            this.cardinalityEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.cardinalityEstimatorProvider);
            this.predicateSelectivityProvider =
                    new MapBasedKeyValueProvider<>(this.parent.predicateSelectivityProvider);
            this.multimapSelectivityProvider =
                    new MapBasedKeyValueProvider<>(this.parent.multimapSelectivityProvider);

            // Providers for cost functions.
            this.operatorLoadProfileEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.operatorLoadProfileEstimatorProvider);
            this.functionLoadProfileEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.functionLoadProfileEstimatorProvider);
            this.loadProfileToTimeConverterProvider =
                    new ConstantProvider<>(this.parent.loadProfileToTimeConverterProvider);

            // Providers for plan enumeration.
            this.pruningStrategiesProvider = new CollectionProvider<>(this.parent.pruningStrategiesProvider);
            this.timeEstimateComparatorProvider = new ConstantProvider<>(this.parent.timeEstimateComparatorProvider);
            this.instrumentationStrategyProvider = new ConstantProvider<>(
                    this.parent.instrumentationStrategyProvider);

        }
    }

    public static Configuration createDefaultConfiguration(RheemContext rheemContext) {
        Configuration configuration = new Configuration(rheemContext);
        bootstrapPlatforms(configuration);
        bootstrapCardinalityEstimationProvider(configuration);
        bootstrapSelectivityProviders(configuration);
        bootstrapLoadAndTimeEstimatorProviders(configuration);
        bootstrapPruningProviders(configuration);
        return configuration;
    }

    private static void bootstrapPlatforms(Configuration configuration) {
        CollectionProvider<Platform> platformProvider = new CollectionProvider<>();
        Platform platform = Platform.load(BASIC_PLATFORM);
        platformProvider.addToWhitelist(platform);
        configuration.setPlatformProvider(platformProvider);
    }

    private static void bootstrapCardinalityEstimationProvider(final Configuration configuration) {
        // Safety net: provide a fallback estimator.
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> fallbackProvider =
                new FunctionalKeyValueProvider<OutputSlot<?>, CardinalityEstimator>(
                        outputSlot -> new FallbackCardinalityEstimator()
                ).withSlf4jWarning("Creating fallback estimator for {}.");

        // Default option: Implementations define their estimators.
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> defaultProvider =
                new FunctionalKeyValueProvider<>(fallbackProvider, (outputSlot, requestee) -> {
                    assert outputSlot.getOwner().isElementary()
                            : String.format("Cannot provide estimator for composite %s.", outputSlot.getOwner());
                    return ((ElementaryOperator) outputSlot.getOwner())
                            .getCardinalityEstimator(outputSlot.getIndex(), configuration)
                            .orElse(null);
                });

        // Customizable layer: Users can override manually.
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> overrideProvider =
                new MapBasedKeyValueProvider<>(defaultProvider);

        configuration.setCardinalityEstimatorProvider(overrideProvider);
    }

    private static void bootstrapSelectivityProviders(Configuration configuration) {
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<PredicateDescriptor, Double> fallbackProvider =
                    new FunctionalKeyValueProvider<PredicateDescriptor, Double>(
                            predicateClass -> 0.5d
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            KeyValueProvider<PredicateDescriptor, Double> overrideProvider =
                    new MapBasedKeyValueProvider<>(fallbackProvider);

            configuration.setPredicateSelectivityProvider(overrideProvider);
        }
        {
            // No safety net here.

            // Customizable layer: Users can override manually.
            KeyValueProvider<FlatMapDescriptor<?, ?>, Double> overrideProvider =
                    new MapBasedKeyValueProvider<>(null);

            configuration.setMultimapSelectivityProvider(overrideProvider);
        }
    }

    private static void bootstrapLoadAndTimeEstimatorProviders(Configuration configuration) {
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> fallbackProvider =
                    new FunctionalKeyValueProvider<ExecutionOperator, LoadProfileEstimator>(
                            operator -> new NestableLoadProfileEstimator(
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000)
                            )
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> overrideProvider =
                    new MapBasedKeyValueProvider<>(fallbackProvider);

            configuration.setOperatorLoadProfileEstimatorProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> fallbackProvider =
                    new FunctionalKeyValueProvider<FunctionDescriptor, LoadProfileEstimator>(
                            operator -> new NestableLoadProfileEstimator(
                                    DefaultLoadEstimator.createIOLinearEstimator(10000),
                                    DefaultLoadEstimator.createIOLinearEstimator(10000),
                                    null, null
                            )
                    ).withSlf4jWarning("Creating fallback selectivity for {}.");

            // Customizable layer: Users can override manually.
            KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> overrideProvider =
                    new MapBasedKeyValueProvider<>(fallbackProvider);

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
        {
            ConstantProvider<Comparator<TimeEstimate>> defaultProvider =
                    new ConstantProvider<>(TimeEstimate.expectionValueComparator());
            ConstantProvider<Comparator<TimeEstimate>> overrideProvider =
                    new ConstantProvider<>(defaultProvider);
            configuration.setTimeEstimateComparatorProvider(overrideProvider);
        }
    }

    private static void bootstrapPruningProviders(Configuration configuration) {
        {
            // By default, no pruning is applied.
            CollectionProvider<PlanEnumerationPruningStrategy> defaultProvider =
                    new CollectionProvider<>();
            configuration.setPruningStrategiesProvider(defaultProvider);

        }
        {
            ConstantProvider<Comparator<TimeEstimate>> defaultProvider =
                    new ConstantProvider<>(TimeEstimate.expectionValueComparator());
            ConstantProvider<Comparator<TimeEstimate>> overrideProvider =
                    new ConstantProvider<>(defaultProvider);
            configuration.setTimeEstimateComparatorProvider(overrideProvider);
        }
        {
            ConstantProvider<InstrumentationStrategy> defaultProvider =
                    new ConstantProvider<>(new OutboundInstrumentationStrategy());
            configuration.setInstrumentationStrategyProvider(defaultProvider);
        }
    }


    /**
     * Creates a child instance.
     */
    public Configuration fork() {
        return new Configuration(this);
    }

    public RheemContext getRheemContext() {
        return this.rheemContext;
    }

    public KeyValueProvider<OutputSlot<?>, CardinalityEstimator> getCardinalityEstimatorProvider() {
        return this.cardinalityEstimatorProvider;
    }

    public void setCardinalityEstimatorProvider(
            KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider) {
        this.cardinalityEstimatorProvider = cardinalityEstimatorProvider;
    }

    public KeyValueProvider<PredicateDescriptor, Double> getPredicateSelectivityProvider() {
        return this.predicateSelectivityProvider;
    }

    public void setPredicateSelectivityProvider(
            KeyValueProvider<PredicateDescriptor, Double> predicateSelectivityProvider) {
        this.predicateSelectivityProvider = predicateSelectivityProvider;
    }

    public KeyValueProvider<FlatMapDescriptor<?, ?>, Double> getMultimapSelectivityProvider() {
        return this.multimapSelectivityProvider;
    }

    public void setMultimapSelectivityProvider(
            KeyValueProvider<FlatMapDescriptor<?, ?>, Double> multimapSelectivityProvider) {
        this.multimapSelectivityProvider = multimapSelectivityProvider;
    }

    public KeyValueProvider<ExecutionOperator, LoadProfileEstimator> getOperatorLoadProfileEstimatorProvider() {
        return this.operatorLoadProfileEstimatorProvider;
    }

    public void setOperatorLoadProfileEstimatorProvider(KeyValueProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider) {
        this.operatorLoadProfileEstimatorProvider = operatorLoadProfileEstimatorProvider;
    }

    public ConstantProvider<LoadProfileToTimeConverter> getLoadProfileToTimeConverterProvider() {
        return this.loadProfileToTimeConverterProvider;
    }

    public void setLoadProfileToTimeConverterProvider(ConstantProvider<LoadProfileToTimeConverter> loadProfileToTimeConverterProvider) {
        this.loadProfileToTimeConverterProvider = loadProfileToTimeConverterProvider;
    }

    public KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> getFunctionLoadProfileEstimatorProvider() {
        return this.functionLoadProfileEstimatorProvider;
    }

    public void setFunctionLoadProfileEstimatorProvider(KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider) {
        this.functionLoadProfileEstimatorProvider = functionLoadProfileEstimatorProvider;
    }

    public CollectionProvider<Platform> getPlatformProvider() {
        return this.platformProvider;
    }

    public void setPlatformProvider(CollectionProvider<Platform> platformProvider) {
        this.platformProvider = platformProvider;
    }

    public ConstantProvider<Comparator<TimeEstimate>> getTimeEstimateComparatorProvider() {
        return this.timeEstimateComparatorProvider;
    }

    public void setTimeEstimateComparatorProvider(ConstantProvider<Comparator<TimeEstimate>> timeEstimateComparatorProvider) {
        this.timeEstimateComparatorProvider = timeEstimateComparatorProvider;
    }


    public CollectionProvider<PlanEnumerationPruningStrategy> getPruningStrategiesProvider() {
        return this.pruningStrategiesProvider;
    }

    public void setPruningStrategiesProvider(CollectionProvider<PlanEnumerationPruningStrategy> pruningStrategiesProvider) {
        this.pruningStrategiesProvider = pruningStrategiesProvider;
    }

    public ConstantProvider<InstrumentationStrategy> getInstrumentationStrategyProvider() {
        return this.instrumentationStrategyProvider;
    }

    public void setInstrumentationStrategyProvider(ConstantProvider<InstrumentationStrategy> instrumentationStrategyProvider) {
        this.instrumentationStrategyProvider = instrumentationStrategyProvider;
    }
}
