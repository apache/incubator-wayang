package org.qcri.rheem.core.platform;

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
import java.util.List;
import java.util.stream.Collectors;

/**
 * Captures data of a execution of a set of {@link ExecutionOperator}s.
 */
public class PartialExecution implements JsonSerializable {

    private final long measuredExecutionTime;

    transient private final Collection<OptimizationContext.OperatorContext> operatorContexts;

    private Collection<OperatorExecution> operatorExecutions;

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
        return new PartialExecution(
                jsonObject.getLong("millis"),
                JsonSerializables.deserializeAllAsList(jsonObject.getJSONArray("executions"), OperatorExecution.class)
        );
    }

    public Collection<OperatorExecution> getOperatorExecutions() {
        if (this.operatorExecutions == null) {
            this.operatorExecutions = this.operatorContexts.stream().map(OperatorExecution::new).collect(Collectors.toList());
        }
        return this.operatorExecutions;
    }

    /**
     * Converts this instance into a {@link JSONObject}.
     *
     * @return the {@link JSONObject}
     */
    @Override
    public JSONObject toJson() {
        final JSONObject jsonThis = new JSONObject();
        jsonThis.put("millis", this.measuredExecutionTime);
        jsonThis.put("executions", JsonSerializables.serializeAll(this.getOperatorExecutions()));
        return jsonThis;
    }

    /**
     * This class reflects one or more executions of an {@link ExecutionOperator}.
     */
    public static class OperatorExecution implements JsonSerializable {

        public OperatorExecution(OptimizationContext.OperatorContext opCtx) {
            this.operator = (ExecutionOperator) opCtx.getOperator();
            this.inputCardinalities = opCtx.getInputCardinalities();
            this.outputCardinalities = opCtx.getOutputCardinalities();
            this.nestedLoadProfile = null;
            this.numExecutions = opCtx.getNumExecutions();
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
            return operatorExecution;
        }

        @Override
        public String toString() {
            return "OperatorExecution[" + operator + ']';
        }
    }
}
