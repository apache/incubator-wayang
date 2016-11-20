package org.qcri.rheem.core.optimizer.costs;

import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;

import java.util.List;

/**
 * This {@link EstimationContext} implementation just stores all required variables without any further logic.
 */
public class SimpleEstimationContext implements EstimationContext {

    private final CardinalityEstimate[] inputCardinalities, outputCardinalities;

    private final TObjectDoubleHashMap<String> doubleProperties;

    private final int numExecutions;

    /**
     * Creates a new instance.
     */
    public SimpleEstimationContext(CardinalityEstimate[] inputCardinalities,
                                   CardinalityEstimate[] outputCardinalities,
                                   TObjectDoubleHashMap<String> doubleProperties,
                                   int numExecutions) {
        this.inputCardinalities = inputCardinalities;
        this.outputCardinalities = outputCardinalities;
        this.doubleProperties = doubleProperties;
        this.numExecutions = numExecutions;
    }

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
        return this.doubleProperties.containsKey(propertyKey) ?
                this.doubleProperties.get(propertyKey) :
                fallback;
    }

    @Override
    public int getNumExecutions() {
        return this.numExecutions;
    }

    /**
     * {@link JsonSerializer} for {@link SimpleEstimationContext}s.
     */
    public static JsonSerializer<SimpleEstimationContext> jsonSerializer =
            new JsonSerializer<SimpleEstimationContext>() {

                @Override
                public JSONObject serialize(SimpleEstimationContext ctx) {
                    return EstimationContext.defaultSerializer.serialize(ctx);
                }

                @Override
                public SimpleEstimationContext deserialize(JSONObject json) {
                    return this.deserialize(json, SimpleEstimationContext.class);
                }

                @Override
                public SimpleEstimationContext deserialize(JSONObject json, Class<? extends SimpleEstimationContext> cls) {
                    final List<CardinalityEstimate> inCards = JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("inCards"),
                            CardinalityEstimate.class
                    );
                    final List<CardinalityEstimate> outCards = JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("outCards"),
                            CardinalityEstimate.class
                    );

                    // TODO: Deserialize double properties.
                    final TObjectDoubleHashMap<String> doubleProperties = new TObjectDoubleHashMap<>();

                    final int numExecutions = json.getInt("executions");

                    return new SimpleEstimationContext(
                            inCards.toArray(new CardinalityEstimate[inCards.size()]),
                            outCards.toArray(new CardinalityEstimate[outCards.size()]),
                            doubleProperties,
                            numExecutions
                    );
                }
            };
}
