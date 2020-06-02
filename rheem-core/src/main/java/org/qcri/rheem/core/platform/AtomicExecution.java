package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.SerializationException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.ConstantLoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimators;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;

/**
 * An atomic execution describes the smallest work unit considered by Rheem's cost model.
 */
public class AtomicExecution {

    /**
     * Estimates the {@link LoadProfile} of the execution unit.
     */
    private LoadProfileEstimator loadProfileEstimator;

    /**
     * Creates a new instance with the given {@link LoadProfileEstimator}.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator}
     */
    public AtomicExecution(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    /**
     * Estimates the {@link LoadProfile} for this instance under a given {@link EstimationContext}.
     *
     * @param context the {@link EstimationContext}
     * @return the {@link LoadProfile}
     */
    public LoadProfile estimateLoad(EstimationContext context) {
        return this.loadProfileEstimator.estimate(context);
    }

    /**
     * This {@link JsonSerializer} stores the given instances via their {@link org.qcri.rheem.core.api.Configuration}
     * key, if any, or else by the {@link LoadProfile} that they estimate.
     */
    public static class KeyOrLoadSerializer implements JsonSerializer<AtomicExecution> {

        private final Configuration configuration;

        private final EstimationContext estimationContext;

        /**
         * Creates a new instance.
         *
         * @param configuration     required for deserialization; can otherwise be {@code null}
         * @param estimationContext of the enclosing {@link AtomicExecutionGroup};
         *                          required for serialization; can otherwise be {@code null}
         */
        public KeyOrLoadSerializer(Configuration configuration, EstimationContext estimationContext) {
            this.configuration = configuration;
            this.estimationContext = estimationContext;
        }

        @Override
        public JSONObject serialize(AtomicExecution atomicExecution) {
            JSONArray estimators = new JSONArray();
            this.serialize(atomicExecution.loadProfileEstimator, estimators);
            return new JSONObject().put("estimators", JsonSerializables.serialize(estimators, false));
        }

        private void serialize(LoadProfileEstimator estimator, JSONArray collector) {
            JSONObject json = new JSONObject();
            if (estimator.getConfigurationKey() != null) {
                json.put("key", estimator.getConfigurationKey());
            } else {
                final LoadProfile loadProfile = estimator.estimate(this.estimationContext);
                json.put("load", JsonSerializables.serialize(loadProfile, false));
            }
            collector.put(json);
            for (LoadProfileEstimator nestedEstimator : estimator.getNestedEstimators()) {
                this.serialize(nestedEstimator, collector);
            }
        }

        @Override
        public AtomicExecution deserialize(JSONObject json) {
            return this.deserialize(json, AtomicExecution.class);
        }

        @Override
        public AtomicExecution deserialize(JSONObject json, Class<? extends AtomicExecution> cls) {
            final JSONArray estimators = json.getJSONArray("estimators");
            if (estimators.length() < 1) {
                throw new IllegalStateException("Expected at least one serialized estimator.");
            }
            // De-serialize the main estimator.
            final JSONObject mainEstimatorJson = estimators.getJSONObject(0);
            LoadProfileEstimator mainEstimator = this.deserializeEstimator(mainEstimatorJson);

            // De-serialize nested estimators.
            for (int i = 1; i < estimators.length(); i++) {
                mainEstimator.nest(this.deserializeEstimator(estimators.getJSONObject(i)));
            }

            return new AtomicExecution(mainEstimator);
        }

        /**
         * Deserialize a {@link LoadProfileEstimator} according to {@link #serialize(LoadProfileEstimator, JSONArray)}.
         *
         * @param jsonObject that should be deserialized
         * @return the {@link LoadProfileEstimator}
         */
        private LoadProfileEstimator deserializeEstimator(JSONObject jsonObject) {
            if (jsonObject.has("key")) {
                final String key = jsonObject.getString("key");
                final LoadProfileEstimator estimator = LoadProfileEstimators.createFromSpecification(key, this.configuration);
                if (estimator == null) {
                    throw new SerializationException("Could not create estimator for key " + key);
                }
                return estimator;
            } else if (jsonObject.has("load")) {
                final LoadProfile load = JsonSerializables.deserialize(jsonObject.getJSONObject("load"), LoadProfile.class);
                return new ConstantLoadProfileEstimator(load);
            }
            throw new SerializationException(String.format("Cannot deserialize load estimator from %s.", jsonObject));
        }
    }

    /**
     * Retrieve the {@link LoadProfileEstimator} encapsulated by this instance.
     *
     * @return the {@link LoadProfileEstimator}
     */
    public LoadProfileEstimator getLoadProfileEstimator() {
        return this.loadProfileEstimator;
    }

    /**
     * Change the {@link LoadProfileEstimator} encapsulated by this instance.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator}
     */
    public void setLoadProfileEstimator(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append('[');
        if (this.loadProfileEstimator.getConfigurationKey() != null) {
            sb.append(this.loadProfileEstimator.getConfigurationKey());
        } else {
            sb.append(this.loadProfileEstimator);
        }
        if (!this.loadProfileEstimator.getNestedEstimators().isEmpty()) {
            sb.append('+').append(this.loadProfileEstimator.getNestedEstimators().size()).append(" nested");
        }
        sb.append(']');
        return sb.toString();
    }
}
