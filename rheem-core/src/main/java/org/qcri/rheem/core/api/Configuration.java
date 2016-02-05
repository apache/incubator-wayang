package org.qcri.rheem.core.api;

import org.qcri.rheem.core.api.configuration.ConfigurationProvider;
import org.qcri.rheem.core.api.configuration.MapBasedConfigurationProvider;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
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

    public Configuration(RheemContext rheemContext) {
        this(rheemContext, null);
    }

    private Configuration(Configuration parent) {
        this(parent.rheemContext, parent);
    }

    private Configuration(RheemContext rheemContext, Configuration parent) {
        this.rheemContext = rheemContext;
        this.parent = parent;

        if (this.parent != null) {
            this.cardinalityEstimatorProvider = new MapBasedConfigurationProvider<>(this.parent.cardinalityEstimatorProvider);
            this.predicateSelectivityProvider = new MapBasedConfigurationProvider<>(this.parent.predicateSelectivityProvider);
            this.multimapSelectivityProvider = new MapBasedConfigurationProvider<>(this.parent.multimapSelectivityProvider);
        }
    }

    public Configuration fork() {
        return new Configuration(this);
    }

    public RheemContext getRheemContext() {
        return rheemContext;
    }

    public ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> getCardinalityEstimatorProvider() {
        return cardinalityEstimatorProvider;
    }

    public void setCardinalityEstimatorProvider(ConfigurationProvider<OutputSlot<?>, CardinalityEstimator> cardinalityEstimatorProvider) {
        this.cardinalityEstimatorProvider = cardinalityEstimatorProvider;
    }

    public ConfigurationProvider<Class<? extends Predicate>, Double> getPredicateSelectivityProvider() {
        return predicateSelectivityProvider;
    }

    public void setPredicateSelectivityProvider(ConfigurationProvider<Class<? extends Predicate>, Double> predicateSelectivityProvider) {
        this.predicateSelectivityProvider = predicateSelectivityProvider;
    }

    public ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> getMultimapSelectivityProvider() {
        return multimapSelectivityProvider;
    }

    public void setMultimapSelectivityProvider(ConfigurationProvider<TransformationDescriptor<?, ? extends Stream<?>>, Double> multimapSelectivityProvider) {
        this.multimapSelectivityProvider = multimapSelectivityProvider;
    }
}
