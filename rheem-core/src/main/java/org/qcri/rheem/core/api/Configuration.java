package org.qcri.rheem.core.api;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.qcri.rheem.core.api.configuration.*;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.ProbabilisticIntervalEstimate;
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
import org.qcri.rheem.core.util.*;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import java.util.Optional;
import java.util.function.Function;

/**
 * Describes both the configuration of a {@link RheemContext} and {@link Job}s.
 */
public class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static final String DEFAULT_CONFIGURATION_FILE = "rheem-core-defaults.properties";

    private static final Configuration defaultConfiguration = new Configuration((Configuration) null);

    static {
        Actions.doSafe(() -> bootstrapCardinalityEstimationProvider(defaultConfiguration));
        Actions.doSafe(() -> bootstrapSelectivityProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapLoadAndTimeEstimatorProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapPruningProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapProperties(defaultConfiguration));
        Actions.doSafe(() -> bootstrapPlatforms(defaultConfiguration));
    }

    private static final String BASIC_PLATFORM = "org.qcri.rheem.basic.plugin.RheemBasicPlatform";

    private final Configuration parent;

    private KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider;

    private KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> predicateSelectivityProvider;

    private KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> multimapSelectivityProvider;

    private KeyValueProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider;

    private KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider;

    private KeyValueProvider<Platform, LoadProfileToTimeConverter> loadProfileToTimeConverterProvider;

    private KeyValueProvider<Platform, Long> platformStartUpTimeProvider;

    private ExplicitCollectionProvider<Platform> platformProvider;

    private ConstantProvider<Comparator<TimeEstimate>> timeEstimateComparatorProvider;

    private CollectionProvider<Class<PlanEnumerationPruningStrategy>> pruningStrategyClassProvider;

    private ConstantProvider<InstrumentationStrategy> instrumentationStrategyProvider;

    private KeyValueProvider<String, String> properties;

    /**
     * Creates a new top-level instance that bases directly from the default instance. Will try to load the
     * user configuration file.
     *
     * @see #getDefaultConfiguration()
     */
    public Configuration() {
        this(findUserConfigurationFile());
    }

    /**
     * Creates a new top-level instance that bases directly from the default instance and loads the specified
     * configuration file.
     *
     * @see #getDefaultConfiguration()
     * @see #load(String)
     */
    public Configuration(String configurationFileUrl) {
        this(getDefaultConfiguration());
        if (configurationFileUrl != null) {
            this.load(configurationFileUrl);
        }
    }

    /**
     * Basic constructor.
     */
    private Configuration(Configuration parent) {
        this.parent = parent;

        if (this.parent != null) {
            // Providers for platforms.
            this.platformProvider = new ExplicitCollectionProvider<>(this, this.parent.platformProvider);

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
                    new MapBasedKeyValueProvider<>(this.parent.loadProfileToTimeConverterProvider);
            this.platformStartUpTimeProvider =
                    new MapBasedKeyValueProvider<>(this.parent.platformStartUpTimeProvider);

            // Providers for plan enumeration.
            this.pruningStrategyClassProvider = new ExplicitCollectionProvider<>(this, this.parent.pruningStrategyClassProvider);
            this.timeEstimateComparatorProvider = new ConstantProvider<>(this.parent.timeEstimateComparatorProvider);
            this.instrumentationStrategyProvider = new ConstantProvider<>(
                    this.parent.instrumentationStrategyProvider);

            // Properties.
            this.properties = new MapBasedKeyValueProvider<>(this.parent.properties);

        }
    }

    private static String findUserConfigurationFile() {
        final String systemProperty = System.getProperty("rheem.configuration");
        if (systemProperty != null) {
            return systemProperty;
        }

        final URL classPathResource = ReflectionUtils.getResourceURL("rheem.properties");
        if (classPathResource != null) {
            return classPathResource.toString();
        }

        return null;
    }

    /**
     * Adjusts this instance to the properties specified in the given file.
     *
     * @param configurationUrl URL to the configuration file
     */
    public void load(String configurationUrl) {
        final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(configurationUrl);
        if (!fileSystem.isPresent()) {
            throw new RheemException(String.format("Could not access %s.", configurationUrl));
        }
        try (InputStream configInputStream = fileSystem.get().open(configurationUrl)) {
            this.load(configInputStream);
        } catch (Exception e) {
            throw new RheemException(String.format("Could not load configuration from %s.", configurationUrl), e);
        }
    }

    /**
     * Adjusts this instance to the properties specified in the given file.
     *
     * @param configInputStream of the file
     */
    public void load(InputStream configInputStream) {
        try {
            final Properties properties = new Properties();
            properties.load(configInputStream);
            for (Map.Entry<Object, Object> propertyEntry : properties.entrySet()) {
                final String key = propertyEntry.getKey().toString();
                final String value = propertyEntry.getValue().toString();
                this.handleConfigurationFileEntry(key, value);
            }
        } catch (IOException e) {
            throw new RheemException("Could not load configuration.", e);
        } finally {
            IOUtils.closeQuietly(configInputStream);
        }
    }

    /**
     * Handle a just loaded property.
     *
     * @param key   the property's key
     * @param value the property's value
     */
    private void handleConfigurationFileEntry(String key, String value) {
        // For now, we just add each entry into the #properties.
        this.setProperty(key, value);
    }


    /**
     * Returns the global default instance. It will be the fallback for all other instances and should only modified
     * to provide default values.
     */
    public static Configuration getDefaultConfiguration() {
        return defaultConfiguration;
    }

    private static void bootstrapPlatforms(Configuration configuration) {
        ExplicitCollectionProvider<Platform> platformProvider = new ExplicitCollectionProvider<>(configuration);
        try {
            Platform platform = Platform.load(BASIC_PLATFORM);
            platformProvider.addToWhitelist(platform);
        } catch (Exception e) {
            LoggerFactory.getLogger(Configuration.class).error("Could not load Rheem basic.");
        }
        configuration.setPlatformProvider(platformProvider);
    }

    private static void bootstrapCardinalityEstimationProvider(final Configuration configuration) {
        // Safety net: provide a fallback estimator.
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> fallbackProvider =
                new FunctionalKeyValueProvider<OutputSlot<?>, CardinalityEstimator>(
                        outputSlot -> new FallbackCardinalityEstimator(),
                        configuration
                ).withSlf4jWarning("Creating fallback cardinality estimator for {}.");

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
        // Selectivity of PredicateDescriptors
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> fallbackProvider =
                    new FunctionalKeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval>(
                            predicateClass -> new ProbabilisticDoubleInterval(0.1, 1, 0.9d),
                            configuration
                    ).withSlf4jWarning("Using fallback selectivity for {}.");

            // Built-in option: Let the PredicateDescriptor provide its selectivity.
            KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            predicateDescriptor ->  predicateDescriptor.getSelectivity().orElse(null)
                    );

            // Customizable layer: Users can override manually.
            KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider);

            configuration.setPredicateSelectivityProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> fallbackProvider =
                    new FunctionalKeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval>(
                            flatMapDescriptor -> new ProbabilisticDoubleInterval(0.1, 100, 0.9d),
                            configuration
                    ).withSlf4jWarning("Using fallback selectivity for {}.");

            // Built-in option: Let the FlatMapDescriptor provide its selectivity.
            KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            flatMapDescriptor -> flatMapDescriptor.getSelectivity().orElse(null)
                    );

            // Customizable layer: Users can override manually.
            KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider, false);

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
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000),
                                    DefaultLoadEstimator.createIOLinearEstimator(operator, 1000)
                            ),
                            configuration
                    ).withSlf4jWarning("Creating fallback load estimator for {}.");

            // Built-in option: let the ExecutionOperators provide the LoadProfileEstimator.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            operator -> operator.getLoadProfileEstimator(configuration).orElse(null)
                    );

            // Customizable layer: Users can override manually.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider);

            configuration.setOperatorLoadProfileEstimatorProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> fallbackProvider =
                    new FunctionalKeyValueProvider<FunctionDescriptor, LoadProfileEstimator>(
                            functionDescriptor -> new NestableLoadProfileEstimator(
                                    DefaultLoadEstimator.createIOLinearEstimator(200),
                                    DefaultLoadEstimator.createIOLinearEstimator(100)
                            ),
                            configuration
                    ).withSlf4jWarning("Creating fallback load estimator for {}.");

            // Built-in layer: let the FunctionDescriptors provide the LoadProfileEstimators themselves.
            KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            functionDescriptor -> functionDescriptor.getLoadProfileEstimator().orElse(null)
                    );

            // Customizable layer: Users can override manually.
            KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider);

            configuration.setFunctionLoadProfileEstimatorProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback start up costs.
            final KeyValueProvider<Platform, Long> fallbackProvider =
                    new FunctionalKeyValueProvider<Platform, Long>(platform -> 0L, configuration)
                            .withSlf4jWarning("Using fallback start up cost provider for {}.");
            KeyValueProvider<Platform, Long> overrideProvider = new MapBasedKeyValueProvider<>(fallbackProvider);
            configuration.setPlatformStartUpTimeProvider(overrideProvider);
        }
        {
            // Safety net: provide a fallback start up costs.
            final KeyValueProvider<Platform, LoadProfileToTimeConverter> fallbackProvider =
                    new FunctionalKeyValueProvider<Platform, LoadProfileToTimeConverter>(
                            platform -> LoadProfileToTimeConverter.createDefault(
                                    LoadToTimeConverter.createLinearCoverter(0.0000005), // 1 CPU with 2 GHz
                                    LoadToTimeConverter.createLinearCoverter(0.00001), // 10 ms to read/write 1 MB
                                    LoadToTimeConverter.createLinearCoverter(0.00001),  // 10 ms to receive/send 1 MB
                                    (cpuEstimate, diskEstimate, networkEstimate) -> cpuEstimate.plus(diskEstimate).plus(networkEstimate)
                            ),
                            configuration
                    )
                            .withSlf4jWarning("Using fallback load-to-time converter for {}.");
            final KeyValueProvider<Platform, LoadProfileToTimeConverter> defaultProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            (platform, requestee) -> platform.createLoadProfileToTimeConverter(
                                    requestee.getConfiguration()
                            )
                    );
            final KeyValueProvider<Platform, LoadProfileToTimeConverter> overrideProvider =
                    new MapBasedKeyValueProvider<>(defaultProvider, false);
            configuration.setLoadProfileToTimeConverterProvider(overrideProvider);
        }
        {
            ConstantProvider<Comparator<TimeEstimate>> defaultProvider =
                    new ConstantProvider<>(TimeEstimate.expectationValueComparator());
            ConstantProvider<Comparator<TimeEstimate>> overrideProvider =
                    new ConstantProvider<>(defaultProvider);
            configuration.setTimeEstimateComparatorProvider(overrideProvider);
        }
    }

    private static void bootstrapPruningProviders(Configuration configuration) {
        {
            // By default, load pruning from the rheem.core.optimizer.pruning.strategies property.
            CollectionProvider<Class<PlanEnumerationPruningStrategy>> propertyBasedProvider =
                    new FunctionalCollectionProvider<>(
                            config -> {
                                final String strategyClassNames = config.getStringProperty("rheem.core.optimizer.pruning.strategies");
                                if (strategyClassNames == null || strategyClassNames.isEmpty()) {
                                    return Collections.emptySet();
                                }
                                Collection<Class<PlanEnumerationPruningStrategy>> strategyClasses = new LinkedList<>();
                                for (String strategyClassName : strategyClassNames.split(",")) {
                                    try {
                                        @SuppressWarnings("unchecked")
                                        final Class<PlanEnumerationPruningStrategy> strategyClass = (Class<PlanEnumerationPruningStrategy>) Class.forName(strategyClassName);
                                        strategyClasses.add(strategyClass);
                                    } catch (ClassNotFoundException e) {
                                        logger.warn("Illegal pruning strategy class name: \"{}\".", strategyClassName);
                                    }
                                }
                                return strategyClasses;
                            },
                            configuration
                    );
            CollectionProvider<Class<PlanEnumerationPruningStrategy>> overrideProvider =
                    new ExplicitCollectionProvider<>(configuration, propertyBasedProvider);
            configuration.setPruningStrategyClassProvider(overrideProvider);
        }
        {
            ConstantProvider<Comparator<TimeEstimate>> defaultProvider =
                    new ConstantProvider<>(TimeEstimate.expectationValueComparator());
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

    private static void bootstrapProperties(Configuration configuration) {
        // Here, we could put some default values.
        final KeyValueProvider<String, String> defaultProperties = new MapBasedKeyValueProvider<>(configuration, false);
        configuration.setProperties(defaultProperties);
        configuration.load(ReflectionUtils.loadResource(DEFAULT_CONFIGURATION_FILE));

        // Set some dynamic properties.
        configuration.setProperty("rheem.core.log.cardinalities", StringUtils.join(
                Arrays.asList(System.getProperty("user.home"), ".rheem", "cardinalities.json"),
                File.separator
        ));
        configuration.setProperty("rheem.core.log.executions", StringUtils.join(
                Arrays.asList(System.getProperty("user.home"), ".rheem", "executions.json"),
                File.separator
        ));

        // Supplement with a customizable layer.
        final KeyValueProvider<String, String> customizableProperties = new MapBasedKeyValueProvider<>(defaultProperties);
        configuration.setProperties(customizableProperties);


    }

    /**
     * Creates a child instance.
     */
    public Configuration fork() {
        return new Configuration(this);
    }


    public KeyValueProvider<OutputSlot<?>, CardinalityEstimator> getCardinalityEstimatorProvider() {
        return this.cardinalityEstimatorProvider;
    }

    public void setCardinalityEstimatorProvider(
            KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider) {
        this.cardinalityEstimatorProvider = cardinalityEstimatorProvider;
    }

    public KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> getPredicateSelectivityProvider() {
        return this.predicateSelectivityProvider;
    }

    public void setPredicateSelectivityProvider(
            KeyValueProvider<PredicateDescriptor<?>, ProbabilisticDoubleInterval> predicateSelectivityProvider) {
        this.predicateSelectivityProvider = predicateSelectivityProvider;
    }

    public KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> getMultimapSelectivityProvider() {
        return this.multimapSelectivityProvider;
    }

    public void setMultimapSelectivityProvider(
            KeyValueProvider<FlatMapDescriptor<?, ?>, ProbabilisticDoubleInterval> multimapSelectivityProvider) {
        this.multimapSelectivityProvider = multimapSelectivityProvider;
    }

    public KeyValueProvider<ExecutionOperator, LoadProfileEstimator> getOperatorLoadProfileEstimatorProvider() {
        return this.operatorLoadProfileEstimatorProvider;
    }

    public void setOperatorLoadProfileEstimatorProvider(KeyValueProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider) {
        this.operatorLoadProfileEstimatorProvider = operatorLoadProfileEstimatorProvider;
    }

    public KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> getFunctionLoadProfileEstimatorProvider() {
        return this.functionLoadProfileEstimatorProvider;
    }

    public void setFunctionLoadProfileEstimatorProvider(KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider) {
        this.functionLoadProfileEstimatorProvider = functionLoadProfileEstimatorProvider;
    }

    public ExplicitCollectionProvider<Platform> getPlatformProvider() {
        return this.platformProvider;
    }

    public void setPlatformProvider(ExplicitCollectionProvider<Platform> platformProvider) {
        this.platformProvider = platformProvider;
    }

    public ConstantProvider<Comparator<TimeEstimate>> getTimeEstimateComparatorProvider() {
        return this.timeEstimateComparatorProvider;
    }

    public void setTimeEstimateComparatorProvider(ConstantProvider<Comparator<TimeEstimate>> timeEstimateComparatorProvider) {
        this.timeEstimateComparatorProvider = timeEstimateComparatorProvider;
    }

    public CollectionProvider<Class<PlanEnumerationPruningStrategy>> getPruningStrategyClassProvider() {
        return this.pruningStrategyClassProvider;
    }


    public void setPruningStrategyClassProvider(CollectionProvider<Class<PlanEnumerationPruningStrategy>> pruningStrategyClassProvider) {
        this.pruningStrategyClassProvider = pruningStrategyClassProvider;
    }

    public ConstantProvider<InstrumentationStrategy> getInstrumentationStrategyProvider() {
        return this.instrumentationStrategyProvider;
    }

    public void setInstrumentationStrategyProvider(ConstantProvider<InstrumentationStrategy> instrumentationStrategyProvider) {
        this.instrumentationStrategyProvider = instrumentationStrategyProvider;
    }

    public KeyValueProvider<Platform, Long> getPlatformStartUpTimeProvider() {
        return this.platformStartUpTimeProvider;
    }

    public void setPlatformStartUpTimeProvider(KeyValueProvider<Platform, Long> platformStartUpTimeProvider) {
        this.platformStartUpTimeProvider = platformStartUpTimeProvider;
    }

    public void setProperties(KeyValueProvider<String, String> properties) {
        this.properties = properties;
    }

    public KeyValueProvider<String, String> getProperties() {
        return this.properties;
    }

    public void setProperty(String key, String value) {
        this.properties.set(key, value);
    }

    public String getStringProperty(String key) {
        return this.properties.provideFor(key);
    }

    public Optional<String> getOptionalStringProperty(String key) {
        return this.properties.optionallyProvideFor(key);
    }

    public String getStringProperty(String key, String fallback) {
        return this.getOptionalStringProperty(key).orElse(fallback);
    }

    public KeyValueProvider<Platform, LoadProfileToTimeConverter> getLoadProfileToTimeConverterProvider() {
        return this.loadProfileToTimeConverterProvider;
    }

    public void setLoadProfileToTimeConverterProvider(KeyValueProvider<Platform, LoadProfileToTimeConverter> loadProfileToTimeConverterProvider) {
        this.loadProfileToTimeConverterProvider = loadProfileToTimeConverterProvider;
    }

    public OptionalLong getOptionalLongProperty(String key) {
        final Optional<String> longValue = this.properties.optionallyProvideFor(key);
        if (longValue.isPresent()) {
            return OptionalLong.of(Long.valueOf(longValue.get()));
        } else {
            return OptionalLong.empty();
        }
    }

    public long getLongProperty(String key) {
        return this.getOptionalLongProperty(key).getAsLong();
    }

    public long getLongProperty(String key, long fallback) {
        return this.getOptionalLongProperty(key).orElse(fallback);
    }

    public OptionalDouble getOptionalDoubleProperty(String key) {
        final Optional<String> optionalDouble = this.properties.optionallyProvideFor(key);
        if (optionalDouble.isPresent()) {
            return OptionalDouble.of(Double.valueOf(optionalDouble.get()));
        } else {
            return OptionalDouble.empty();
        }
    }

    public double getDoubleProperty(String key) {
        return this.getOptionalDoubleProperty(key).getAsDouble();
    }

    public double getDoubleProperty(String key, long fallback) {
        return this.getOptionalDoubleProperty(key).orElse(fallback);
    }

    public Optional<Boolean> getOptionalBooleanProperty(String key) {
        return this.properties.optionallyProvideFor(key).map(Boolean::valueOf);
    }

    public boolean getBooleanProperty(String key) {
        return this.getOptionalBooleanProperty(key).get();
    }

    public boolean getBooleanProperty(String key, boolean fallback) {
        return this.getOptionalBooleanProperty(key).orElse(fallback);
    }

    public Configuration getParent() {
        return parent;
    }
}
