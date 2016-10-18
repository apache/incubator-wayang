package org.qcri.rheem.core.platform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.JsonSerializables;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Captures data of a execution of a set of {@link ExecutionOperator}s.
 */
public class PartialExecution implements JsonSerializable {

    /**
     * The measured execution time of this instance in milliseconds.
     */
    private final long measuredExecutionTime;

    /**
     * {@link OptimizationContext.OperatorContext}s captured by this instance.
     */
    transient private final Collection<OptimizationContext.OperatorContext> operatorContexts;

    /**
     * Persistent reflection of the {@link #operatorContexts}.
     */
    private Collection<OperatorExecution> operatorExecutions;

    /**
     * Platforms initialized in this instance.
     */
    private Collection<Platform> initializedPlatforms = new LinkedList<>();

    /**
     * Creates a new instance.
     *
     * @param measuredExecutionTime the time measured for the partial execution
     * @param operatorContexts      for all executed {@link ExecutionOperator}s
     */
    public PartialExecution(long measuredExecutionTime, Collection<OptimizationContext.OperatorContext> operatorContexts) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.operatorContexts = operatorContexts;
    }

    /**
     * Deserialization constructor.
     */
    private PartialExecution(long measuredExecutionTime, List<OperatorExecution> executions) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.operatorContexts = null;
        this.operatorExecutions = executions;
    }

    public long getMeasuredExecutionTime() {
        return measuredExecutionTime;
    }

    public Collection<OptimizationContext.OperatorContext> getOperatorContexts() {
        return operatorContexts;
    }

    /**
     * Calculates the overall {@link TimeEstimate} of this instance.
     *
     * @return the overall {@link TimeEstimate}
     */
    public TimeEstimate getOverallTimeEstimate() {
        return operatorContexts.stream()
                .map(OptimizationContext.OperatorContext::getTimeEstimate)
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
    }

    /**
     * Parses a {@link JSONObject} and creates a new instance.
     *
     * @param jsonObject to be parsed
     * @return the new instance
     */
    public static PartialExecution fromJson(JSONObject jsonObject) {
        final PartialExecution partialExecution = new PartialExecution(
                jsonObject.getLong("millis"),
                JsonSerializables.deserializeAllAsList(jsonObject.getJSONArray("executions"), OperatorExecution.class)
        );
        final JSONArray platforms = jsonObject.optJSONArray("initPlatforms");
        if (platforms != null) {
            for (Object platform : platforms) {
                partialExecution.addInitializedPlatform(Platform.load((String) platform));
            }
        }
        return partialExecution;
    }

    public Collection<OperatorExecution> getOperatorExecutions() {
        if (this.operatorExecutions == null) {
            this.operatorExecutions = this.operatorContexts.stream().map(OperatorExecution::new).collect(Collectors.toList());
        }
        return this.operatorExecutions;
    }

    public Collection<Platform> getInitializedPlatforms() {
        return this.initializedPlatforms;
    }

    public void addInitializedPlatform(Platform platform) {
        this.initializedPlatforms.add(platform);
    }

    /**
     * Converts this instance into a {@link JSONObject}.
     *
     * @return the {@link JSONObject}
     */
    @Override
    public JSONObject toJson() {
        return new JSONObject()
                .put("millis", this.measuredExecutionTime)
                .put("executions", JsonSerializables.serializeAll(this.getOperatorExecutions()))
                .putOpt("initPlatforms", JsonSerializables.serializeAll(
                        this.getInitializedPlatforms().stream()
                                .map(platform -> platform.getClass().getCanonicalName())
                                .collect(Collectors.toList())
                ));
    }

    /**
     * This class reflects one or more executions of an {@link ExecutionOperator}.
     */
    public static class OperatorExecution implements JsonSerializable {

        public OperatorExecution(OptimizationContext.OperatorContext opCtx) {
            this.operator = (ExecutionOperator) opCtx.getOperator();
            this.inputCardinalities = opCtx.getInputCardinalities();
            this.outputCardinalities = opCtx.getOutputCardinalities();
            this.numExecutions = opCtx.getNumExecutions();
            final LoadProfile loadProfile = opCtx.getLoadProfile();
            final Collection<LoadProfile> subprofiles = loadProfile == null ?
                    Collections.emptyList() :
                    loadProfile.getSubprofiles();
            this.nestedLoadProfile = subprofiles.isEmpty() ?
                    null :
                    subprofiles.stream().reduce(LoadProfile.emptyLoadProfile, LoadProfile::plus);
        }

        private OperatorExecution() {

        }

        private ExecutionOperator operator;

        private CardinalityEstimate[] inputCardinalities;

        private CardinalityEstimate[] outputCardinalities;

        private LoadProfile nestedLoadProfile;

        private int numExecutions;

        public ExecutionOperator getOperator() {
            return operator;
        }

        public CardinalityEstimate[] getInputCardinalities() {
            return inputCardinalities;
        }

        public CardinalityEstimate[] getOutputCardinalities() {
            return outputCardinalities;
        }

        public LoadProfile getNestedLoadProfile() {
            return nestedLoadProfile;
        }

        public int getNumExecutions() {
            return numExecutions;
        }

        @Override
        public JSONObject toJson() {
            JSONObject json = new JSONObject();
            json.put("operator", JsonSerializables.serializeAtLeastClass(this.operator));
            json.put("inputCards", JsonSerializables.serializeAll(this.inputCardinalities));
            json.put("outputCards", JsonSerializables.serializeAll(this.outputCardinalities));
            json.put("executions", JsonSerializables.serialize(this.numExecutions));
            json.putOpt("nestedLoadProfile", JsonSerializables.serialize(this.nestedLoadProfile));
            return json;
        }

        @SuppressWarnings("unused")
        public static OperatorExecution fromJson(JSONObject jsonObject) {
            OperatorExecution operatorExecution = new OperatorExecution();
            operatorExecution.operator = JsonSerializables.deserializeAtLeastClass(
                    jsonObject.getJSONObject("operator"), OperatorBase.STANDARD_OPERATOR_ARGS
            );
            operatorExecution.inputCardinalities = JsonSerializables.deserializeAllAsArray(
                    jsonObject.getJSONArray("inputCards"), CardinalityEstimate.class
            );
            operatorExecution.outputCardinalities = JsonSerializables.deserializeAllAsArray(
                    jsonObject.getJSONArray("outputCards"), CardinalityEstimate.class
            );
            operatorExecution.numExecutions = jsonObject.getInt("executions");
            if (jsonObject.has("nestedLoadProfile")) {
                operatorExecution.nestedLoadProfile = JsonSerializables.deserialize(
                        jsonObject.getJSONObject("nestedLoadProfile"), LoadProfile.class
                );
            }
            operatorExecution.nestedLoadProfile = JsonSerializables.deserialize(
                    jsonObject.optJSONObject("nestedLoadProfile"), LoadProfile.class
            );
            return operatorExecution;
        }

        @Override
        public String toString() {
            return "OperatorExecution[" + operator + ']';
        }
    }
}
