package org.qcri.rheem.core.optimizer.costs;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;

import java.util.Collection;
import java.util.List;

/**
 * This {@link EstimationContext} implementation just stores all required variables without any further logic.
 */
public class SimpleEstimationContext implements EstimationContext {

    private final CardinalityEstimate[] inputCardinalities, outputCardinalities;

    private final TObjectDoubleMap<String> doubleProperties;

    private final int numExecutions;

    /**
     * Creates a new instance.
     */
    public SimpleEstimationContext(CardinalityEstimate[] inputCardinalities,
                                   CardinalityEstimate[] outputCardinalities,
                                   TObjectDoubleMap<String> doubleProperties,
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

    @Override
    public Collection<String> getPropertyKeys() {
        return this.doubleProperties.keySet();
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

                    final TObjectDoubleHashMap<String> doubleProperties = new TObjectDoubleHashMap<>();
                    final JSONObject doublePropertiesJson = json.optJSONObject("properties");
                    if (doublePropertiesJson != null) {
                        for (String key : doublePropertiesJson.keySet()) {
                            doubleProperties.put(key, doublePropertiesJson.getDouble(key));
                        }
                    }

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
