package org.qcri.rheem.core.api;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimationProvider;

/**
 * Describes both the configuration of a {@link RheemContext} and {@link Job}s.
 */
public class Configuration {

    private final RheemContext rheemContext;

    private final Configuration parent;

    private final CardinalityEstimationProvider cardinalityEstimationProvider;

    public Configuration(RheemContext rheemContext) {
        this(rheemContext, null);
    }

    private Configuration(Configuration parent) {
        this(parent.rheemContext, parent);
    }

    private Configuration(RheemContext rheemContext, Configuration parent) {
        this.rheemContext = rheemContext;
        this.parent = parent;

        if (this.parent == null) {
            this.cardinalityEstimationProvider = new CardinalityEstimationProvider();
            // ...
        } else {
            this.cardinalityEstimationProvider = this.parent.cardinalityEstimationProvider.createChild();
            // ...
        }
    }

    public Configuration fork() {
        return new Configuration(this);
    }

    public RheemContext getRheemContext() {
        return rheemContext;
    }
}
