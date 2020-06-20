package org.qcri.rheem.core.api;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.qcri.rheem.core.api.configuration.CollectionProvider;
import org.qcri.rheem.core.api.configuration.ConstantValueProvider;
import org.qcri.rheem.core.api.configuration.ExplicitCollectionProvider;
import org.qcri.rheem.core.api.configuration.FunctionalCollectionProvider;
import org.qcri.rheem.core.api.configuration.FunctionalKeyValueProvider;
import org.qcri.rheem.core.api.configuration.FunctionalValueProvider;
import org.qcri.rheem.core.api.configuration.KeyValueProvider;
import org.qcri.rheem.core.api.configuration.MapBasedKeyValueProvider;
import org.qcri.rheem.core.api.configuration.ValueProvider;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.MapPartitionsDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.FallbackCardinalityEstimator;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.optimizer.costs.IntervalLoadEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.LoadToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.NestableLoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.optimizer.enumeration.PlanEnumerationPruningStrategy;
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.profiling.InstrumentationStrategy;
import org.qcri.rheem.core.profiling.OutboundInstrumentationStrategy;
import org.qcri.rheem.core.util.Actions;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Random;
import java.util.function.ToDoubleFunction;

import static org.qcri.rheem.core.util.ReflectionUtils.instantiateDefault;

/**
 * Describes both the configuration of a {@link RheemContext} and {@link Job}s.
 */
public class Configuration {

    private static final Logger logger = LoggerFactory.getLogger(Configuration.class);

    private static final String DEFAULT_CONFIGURATION_FILE = "rheem-core-defaults.properties";

    private static final Configuration defaultConfiguration = new Configuration((Configuration) null);

    static {
        defaultConfiguration.name = "default";
        Actions.doSafe(() -> bootstrapCardinalityEstimationProvider(defaultConfiguration));
        Actions.doSafe(() -> bootstrapSelectivityProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapLoadAndTimeEstimatorProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapPruningProviders(defaultConfiguration));
        Actions.doSafe(() -> bootstrapProperties(defaultConfiguration));
        Actions.doSafe(() -> bootstrapPlugins(defaultConfiguration));
    }

    private static final String BASIC_PLUGIN = "org.qcri.rheem.basic.RheemBasics.defaultPlugin()";

    private String name = "(no name)";

    private final Configuration parent;

    private KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider;

    private KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> udfSelectivityProvider;

    private KeyValueProvider<ExecutionOperator, LoadProfileEstimator> operatorLoadProfileEstimatorProvider;

    private KeyValueProvider<FunctionDescriptor, LoadProfileEstimator> functionLoadProfileEstimatorProvider;

    private MapBasedKeyValueProvider<String, LoadProfileEstimator> loadProfileEstimatorCache;

    private KeyValueProvider<Platform, LoadProfileToTimeConverter> loadProfileToTimeConverterProvider;

    private KeyValueProvider<Platform, TimeToCostConverter> timeToCostConverterProvider;

    private ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>> costSquasherProvider;

    private KeyValueProvider<Platform, Long> platformStartUpTimeProvider;

    private ExplicitCollectionProvider<Platform> platformProvider;

    private ExplicitCollectionProvider<Mapping> mappingProvider;

    private ExplicitCollectionProvider<ChannelConversion> channelConversionProvider;

    private CollectionProvider<Class<PlanEnumerationPruningStrategy>> pruningStrategyClassProvider;

    private ValueProvider<InstrumentationStrategy> instrumentationStrategyProvider;

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
            this.name = configurationFileUrl;
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
            this.mappingProvider = new ExplicitCollectionProvider<>(this, this.parent.mappingProvider);
            this.channelConversionProvider = new ExplicitCollectionProvider<>(this, this.parent.channelConversionProvider);

            // Providers for cardinality estimation.
            this.cardinalityEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.cardinalityEstimatorProvider, this);
            this.udfSelectivityProvider =
                    new MapBasedKeyValueProvider<>(this.parent.udfSelectivityProvider, this);

            // Providers for cost functions.
            this.operatorLoadProfileEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.operatorLoadProfileEstimatorProvider, this);
            this.functionLoadProfileEstimatorProvider =
                    new MapBasedKeyValueProvider<>(this.parent.functionLoadProfileEstimatorProvider, this);
            this.loadProfileEstimatorCache =
                    new MapBasedKeyValueProvider<>(this.parent.loadProfileEstimatorCache, this);
            this.loadProfileToTimeConverterProvider =
                    new MapBasedKeyValueProvider<>(this.parent.loadProfileToTimeConverterProvider, this);
            this.timeToCostConverterProvider =
                    new MapBasedKeyValueProvider<>(this.parent.timeToCostConverterProvider, this);
            this.platformStartUpTimeProvider =
                    new MapBasedKeyValueProvider<>(this.parent.platformStartUpTimeProvider, this);
            this.costSquasherProvider =
                    new ConstantValueProvider<>(this, this.parent.costSquasherProvider);

            // Providers for plan enumeration.
            this.pruningStrategyClassProvider = new ExplicitCollectionProvider<>(this, this.parent.pruningStrategyClassProvider);
            this.instrumentationStrategyProvider = new ConstantValueProvider<>(this, this.parent.instrumentationStrategyProvider);

            // Properties.
            this.properties = new MapBasedKeyValueProvider<>(this.parent.properties, this);

        }
    }

    private static String findUserConfigurationFile() {
        final String systemProperty = System.getProperty("rheem.configuration");
        if (systemProperty != null) {
            logger.info("Using configuration at {}.", systemProperty);
            return systemProperty;
        }

        final URL classPathResource = ReflectionUtils.getResourceURL("rheem.properties");
        if (classPathResource != null) {
            logger.info("Using configuration at {}.", classPathResource);
            return classPathResource.toString();
        }

        logger.info("Using blank configuration.");
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
        switch (key) {
            case "rheem.core.optimizer.cost.squash":
                if (!(this.costSquasherProvider instanceof ConstantValueProvider)) {
                    logger.warn("Cannot update cost estimate provider.");
                } else if ("expectation".equals(value)) {
                    ((ConstantValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>>) this.costSquasherProvider).setValue(
                            ProbabilisticDoubleInterval::getGeometricMeanEstimate
                    );
                } else if ("random".equals(value)) {
                    final int salt = new Random().nextInt();
                    ((ConstantValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>>) this.costSquasherProvider).setValue(
                            cost -> cost.hashCode() * salt + cost.hashCode()
                    );
                } else {
                    logger.warn("Cannot set unknown cost comparator \"{}\".", value);
                }
                break;
            default:
                this.setProperty(key, value);
                break;
        }
    }


    /**
     * Returns the global default instance. It will be the fallback for all other instances and should only modified
     * to provide default values.
     */
    public static Configuration getDefaultConfiguration() {
        return defaultConfiguration;
    }

    private static void bootstrapPlugins(Configuration configuration) {
        configuration.setPlatformProvider(new ExplicitCollectionProvider<>(configuration));
        configuration.setMappingProvider(new ExplicitCollectionProvider<>(configuration));
        configuration.setChannelConversionProvider(new ExplicitCollectionProvider<>(configuration));
        try {
            Plugin basicPlugin = ReflectionUtils.evaluate(BASIC_PLUGIN);
            basicPlugin.configure(configuration);
        } catch (Exception e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not load basic plugin.", e);
            } else {
                logger.warn("Could not load basic plugin.");
            }
        }
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
                    final ElementaryOperator operator = (ElementaryOperator) outputSlot.getOwner();
                    // Instance-level estimator?
                    if (operator.getCardinalityEstimator(outputSlot.getIndex()) != null) {
                        return operator.getCardinalityEstimator(outputSlot.getIndex());
                    }
                    // Type-level estimator?
                    return operator
                            .createCardinalityEstimator(outputSlot.getIndex(), configuration)
                            .orElse(null);
                });

        // Customizable layer: Users can override manually.
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> overrideProvider =
                new MapBasedKeyValueProvider<>(defaultProvider);

        configuration.setCardinalityEstimatorProvider(overrideProvider);
    }

    private static void bootstrapSelectivityProviders(Configuration configuration) {
        // Selectivity of UDFs
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> fallbackProvider =
                    new FunctionalKeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval>(
                            functionDescriptor -> {
                                if (functionDescriptor instanceof PredicateDescriptor) {
                                    return new ProbabilisticDoubleInterval(0.1, 1, 0.9d);
                                } else if (functionDescriptor instanceof FlatMapDescriptor) {
                                    return new ProbabilisticDoubleInterval(0.1, 1, 0.9d);
                                } else if (functionDescriptor instanceof MapPartitionsDescriptor) {
                                    return new ProbabilisticDoubleInterval(0.1, 1, 0.9d);
                                } else {
                                    throw new RheemException("Cannot provide fallback selectivity for " + functionDescriptor);
                                }
                            },
                            configuration
                    ).withSlf4jWarning("Using fallback selectivity for {}.");

            // Built-in option: Let the PredicateDescriptor provide its selectivity.
            KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            functionDescriptor -> FunctionDescriptor.getSelectivity(functionDescriptor).orElse(null)
                    );

            // Customizable layer: Users can override manually.
            KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider);

            configuration.setUdfSelectivityProvider(overrideProvider);
        }
    }

    private static void bootstrapLoadAndTimeEstimatorProviders(Configuration configuration) {
        {
            // Safety net: provide a fallback selectivity.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> fallbackProvider =
                    new FunctionalKeyValueProvider<ExecutionOperator, LoadProfileEstimator>(
                            (operator, requestee) -> {
                                final Configuration conf = requestee.getConfiguration();
                                return new NestableLoadProfileEstimator(
                                        IntervalLoadEstimator.createIOLinearEstimator(
                                                null,
                                                conf.getLongProperty("rheem.core.fallback.udf.cpu.lower"),
                                                conf.getLongProperty("rheem.core.fallback.udf.cpu.upper"),
                                                conf.getDoubleProperty("rheem.core.fallback.udf.cpu.confidence"),
                                                CardinalityEstimate.EMPTY_ESTIMATE
                                        ),
                                        IntervalLoadEstimator.createIOLinearEstimator(
                                                null,
                                                conf.getLongProperty("rheem.core.fallback.udf.ram.lower"),
                                                conf.getLongProperty("rheem.core.fallback.udf.ram.upper"),
                                                conf.getDoubleProperty("rheem.core.fallback.udf.ram.confidence"),
                                                CardinalityEstimate.EMPTY_ESTIMATE
                                        )
                                );
                            },
                            configuration
                    ).withSlf4jWarning("Creating fallback load estimator for {}.");

            // Built-in option: let the ExecutionOperators provide the LoadProfileEstimator.
            KeyValueProvider<ExecutionOperator, LoadProfileEstimator> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            (operator, requestee) -> operator.createLoadProfileEstimator(requestee.getConfiguration()).orElse(null)
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
                            (operator, requestee) -> {
                                final Configuration conf = requestee.getConfiguration();
                                return new NestableLoadProfileEstimator(
                                        IntervalLoadEstimator.createIOLinearEstimator(
                                                null,
                                                conf.getLongProperty("rheem.core.fallback.operator.cpu.lower"),
                                                conf.getLongProperty("rheem.core.fallback.operator.cpu.upper"),
                                                conf.getDoubleProperty("rheem.core.fallback.operator.cpu.confidence"),
                                                CardinalityEstimate.EMPTY_ESTIMATE
                                        ),
                                        IntervalLoadEstimator.createIOLinearEstimator(
                                                null,
                                                conf.getLongProperty("rheem.core.fallback.operator.ram.lower"),
                                                conf.getLongProperty("rheem.core.fallback.operator.ram.upper"),
                                                conf.getDoubleProperty("rheem.core.fallback.operator.ram.confidence"),
                                                CardinalityEstimate.EMPTY_ESTIMATE
                                        )
                                );
                            },
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
            final KeyValueProvider<Platform, Long> builtinProvider = new FunctionalKeyValueProvider<>(
                    (platform, requestee) -> platform.getInitializeMillis(requestee.getConfiguration()),
                    configuration
            );

            // Override layer.
            KeyValueProvider<Platform, Long> overrideProvider = new MapBasedKeyValueProvider<>(builtinProvider);
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
            // Safety net: provide a fallback start up costs.
            final KeyValueProvider<Platform, TimeToCostConverter> fallbackProvider =
                    new FunctionalKeyValueProvider<Platform, TimeToCostConverter>(
                            platform -> new TimeToCostConverter(0d, 1d),
                            configuration
                    ).withSlf4jWarning("Using fallback time-to-cost converter for {}.");
            final KeyValueProvider<Platform, TimeToCostConverter> builtInProvider =
                    new FunctionalKeyValueProvider<>(
                            fallbackProvider,
                            (platform, requestee) -> platform.createTimeToCostConverter(
                                    requestee.getConfiguration()
                            )
                    );
            final KeyValueProvider<Platform, TimeToCostConverter> overrideProvider =
                    new MapBasedKeyValueProvider<>(builtInProvider, false);
            configuration.setTimeToCostConverterProvider(overrideProvider);
        }
        {
            configuration.setLoadProfileEstimatorCache(new MapBasedKeyValueProvider<>(configuration, true));
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
            ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>> defaultProvider =
                    new ConstantValueProvider<>(ProbabilisticDoubleInterval::getGeometricMeanEstimate, configuration);
            ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>> overrideProvider =
                    new ConstantValueProvider<>(defaultProvider);
            configuration.setCostSquasherProvider(overrideProvider);
        }
        {
            ValueProvider<InstrumentationStrategy> defaultProvider =
                    new ConstantValueProvider<>(new OutboundInstrumentationStrategy(), configuration);
            ValueProvider<InstrumentationStrategy> configProvider =
                    new FunctionalValueProvider<>(
                            requestee -> {
                                Optional<String> optInstrumentationtStrategyClass =
                                        requestee.getConfiguration().getOptionalStringProperty("rheem.core.optimizer.instrumentation");
                                if (!optInstrumentationtStrategyClass.isPresent()) {
                                    return null;
                                }
                                return instantiateDefault(optInstrumentationtStrategyClass.get());
                            },
                            defaultProvider
                    );
            ValueProvider<InstrumentationStrategy> overrideProvider = new ConstantValueProvider<>(configProvider);
            configuration.setInstrumentationStrategyProvider(overrideProvider);
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

    /**
     * Creates a child instance.
     *
     * @param name for the child instance
     */
    public Configuration fork(String name) {
        final Configuration configuration = new Configuration(this);
        configuration.name = name;
        return configuration;
    }


    public KeyValueProvider<OutputSlot<?>, CardinalityEstimator> getCardinalityEstimatorProvider() {
        return this.cardinalityEstimatorProvider;
    }

    public void setCardinalityEstimatorProvider(
            KeyValueProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider) {
        this.cardinalityEstimatorProvider = cardinalityEstimatorProvider;
    }

    public KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> getUdfSelectivityProvider() {
        return this.udfSelectivityProvider;
    }

    public void setUdfSelectivityProvider(
            KeyValueProvider<FunctionDescriptor, ProbabilisticDoubleInterval> udfSelectivityProvider) {
        this.udfSelectivityProvider = udfSelectivityProvider;
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

    public MapBasedKeyValueProvider<String, LoadProfileEstimator> getLoadProfileEstimatorCache() {
        return this.loadProfileEstimatorCache;
    }

    public void setLoadProfileEstimatorCache(MapBasedKeyValueProvider<String, LoadProfileEstimator> loadProfileEstimatorCache) {
        this.loadProfileEstimatorCache = loadProfileEstimatorCache;
    }

    public ExplicitCollectionProvider<Platform> getPlatformProvider() {
        return this.platformProvider;
    }

    public void setPlatformProvider(ExplicitCollectionProvider<Platform> platformProvider) {
        this.platformProvider = platformProvider;
    }

    public ExplicitCollectionProvider<Mapping> getMappingProvider() {
        return mappingProvider;
    }

    public void setMappingProvider(ExplicitCollectionProvider<Mapping> mappingProvider) {
        this.mappingProvider = mappingProvider;
    }

    public ExplicitCollectionProvider<ChannelConversion> getChannelConversionProvider() {
        return channelConversionProvider;
    }

    public void setChannelConversionProvider(ExplicitCollectionProvider<ChannelConversion> channelConversionProvider) {
        this.channelConversionProvider = channelConversionProvider;
    }

    public CollectionProvider<Class<PlanEnumerationPruningStrategy>> getPruningStrategyClassProvider() {
        return this.pruningStrategyClassProvider;
    }


    public void setPruningStrategyClassProvider(CollectionProvider<Class<PlanEnumerationPruningStrategy>> pruningStrategyClassProvider) {
        this.pruningStrategyClassProvider = pruningStrategyClassProvider;
    }

    public ValueProvider<InstrumentationStrategy> getInstrumentationStrategyProvider() {
        return this.instrumentationStrategyProvider;
    }

    public void setInstrumentationStrategyProvider(ValueProvider<InstrumentationStrategy> instrumentationStrategyProvider) {
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

    public KeyValueProvider<Platform, TimeToCostConverter> getTimeToCostConverterProvider() {
        return timeToCostConverterProvider;
    }

    public void setTimeToCostConverterProvider(KeyValueProvider<Platform, TimeToCostConverter> timeToCostConverterProvider) {
        this.timeToCostConverterProvider = timeToCostConverterProvider;
    }

    public ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>> getCostSquasherProvider() {
        return this.costSquasherProvider;
    }

    public void setCostSquasherProvider(ValueProvider<ToDoubleFunction<ProbabilisticDoubleInterval>> costSquasherProvider) {
        this.costSquasherProvider = costSquasherProvider;
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
        final OptionalLong optionalLongProperty = this.getOptionalLongProperty(key);
        if (!optionalLongProperty.isPresent()) {
            throw new RheemException(String.format("No value for \"%s\".", key));
        }
        return optionalLongProperty.getAsLong();
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

    public double getDoubleProperty(String key, double fallback) {
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

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.name);
    }

    public String getName() {
        return this.name;
    }
}
