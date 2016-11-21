package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;

import java.util.Arrays;
import java.util.Collection;

/**
 * Provides parameters required by {@link LoadProfileEstimator}s.
 */
public interface EstimationContext {

    /**
     * Provide the input {@link CardinalityEstimate}s for the {@link Operator} that corresponds to this instance.
     *
     * @return the {@link CardinalityEstimate}s, which can contain {@code null}s
     */
    CardinalityEstimate[] getInputCardinalities();

    /**
     * Provide the output {@link CardinalityEstimate}s for the {@link Operator} that corresponds to this instance.
     *
     * @return the {@link CardinalityEstimate}s, which can contain {@code null}s
     */
    CardinalityEstimate[] getOutputCardinalities();

    /**
     * Retrieve a {@code double}-valued property in this context.
     *
     * @param propertyKey the key for the property
     * @param fallback    the value to use if the property could not be delivered
     * @return the property value or {@code fallback}
     */
    double getDoubleProperty(String propertyKey, double fallback);

    /**
     * Retrieve the eligible property keys for {@link #getDoubleProperty(String, double)}.
     *
     * @return the property keys
     */
    Collection<String> getPropertyKeys();

    /**
     * Retrieve the number of executions to be estimated.
     *
     * @return the number of executions.
     */
    int getNumExecutions();

    /**
     * Provide a normalized instance, that has only a single execution. The {@link CardinalityEstimate}s will be
     * adapted accordingly.
     *
     * @return the normalized instance
     * @see #normalize(CardinalityEstimate[], int)
     */
    default EstimationContext getNormalizedEstimationContext() {
        if (this.getNumExecutions() == 1) return this;

        return new EstimationContext() {

            private final CardinalityEstimate[] inputCardinalities = EstimationContext.normalize(
                    EstimationContext.this.getInputCardinalities(),
                    EstimationContext.this.getNumExecutions()
            );

            private final CardinalityEstimate[] outputCardinalities = EstimationContext.normalize(
                    EstimationContext.this.getOutputCardinalities(),
                    EstimationContext.this.getNumExecutions()
            );

            @Override
            public CardinalityEstimate[] getInputCardinalities() {
                return this.inputCardinalities;
            }

            @Override
            public CardinalityEstimate[] getOutputCardinalities() {
                return this.outputCardinalities;
            }

            @Override
            public double getDoubleProperty(String propertyKey, double fallback) {
                return EstimationContext.this.getDoubleProperty(propertyKey, fallback);
            }

            @Override
            public Collection<String> getPropertyKeys() {
                return EstimationContext.this.getPropertyKeys();
            }

            @Override
            public int getNumExecutions() {
                return 1;
            }
        };
    }

    /**
     * Normalize the given estimates by dividing them by a number of executions.
     *
     * @param estimates     that should be normalized
     * @param numExecutions the number execution
     * @return the normalized estimates (and {@code estimates} if {@code numExecution == 1}
     */
    static CardinalityEstimate[] normalize(CardinalityEstimate[] estimates, int numExecutions) {
        if (numExecutions == 1 || estimates.length == 0) return estimates;

        CardinalityEstimate[] normalizedEstimates = new CardinalityEstimate[estimates.length];
        for (int i = 0; i < estimates.length; i++) {
            final CardinalityEstimate estimate = estimates[i];
            if (estimate != null) normalizedEstimates[i] = estimate.divideBy(numExecutions);
        }

        return normalizedEstimates;
    }

    /**
     * Default {@link JsonSerializer} for {@link EstimationContext}s. Does not support deserialization, though.
     */
    JsonSerializer<EstimationContext> defaultSerializer = new JsonSerializer<EstimationContext>() {

        @Override
        public JSONObject serialize(EstimationContext ctx) {
            JSONObject doubleProperties = new JSONObject();
            for (String key : ctx.getPropertyKeys()) {
                double value = ctx.getDoubleProperty(key, 0);
                doubleProperties.put(key, value);
            }
            if (doubleProperties.length() == 0) doubleProperties = null;
            return new JSONObject()
                    .put("inCards", JsonSerializables.serializeAll(Arrays.asList(ctx.getInputCardinalities()), false))
                    .put("outCards", JsonSerializables.serializeAll(Arrays.asList(ctx.getOutputCardinalities()), false))
                    .put("executions", ctx.getNumExecutions())
                    .putOpt("properties", doubleProperties);
        }

        @Override
        public EstimationContext deserialize(JSONObject json, Class<? extends EstimationContext> cls) {
            throw new UnsupportedOperationException("Deserialization not supported.");
        }
    };

}
