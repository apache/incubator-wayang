package org.qcri.rheem.core.platform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
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

    public static class KeyOrLoadSerializer implements JsonSerializer<AtomicExecution> {

        private final EstimationContext estimationContext;

        public KeyOrLoadSerializer(EstimationContext estimationContext) {
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
            // TODO: Deserialize with DynamicLoadProfileEstimator.
            return null;
        }
    }
}
