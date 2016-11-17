package org.qcri.rheem.core.platform;

import org.json.JSONArray;
import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.OperatorBase;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.JsonSerializables;

import java.util.*;
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
     * Cost bounds for this instance.
     */
    private final double lowerCost, upperCost;

    /**
     * {@link AtomicExecutionGroup}s captured by this instance.
     */
    transient private final Collection<AtomicExecutionGroup> atomicExecutionGroups;

    /**
     * Persistent reflection of the {@link #operatorContexts}.
     */
    private Collection<OperatorExecution> operatorExecutions;

    /**
     * Platforms initialized/involved this instance.
     */
    private Collection<Platform> initializedPlatforms = new LinkedList<>(), involvedPlatforms;

    /**
     * Creates a new instance according to the measurement data.
     *
     * @param measuredExecutionTime the measured execution time
     * @param executionLineageNodes the {@link ExecutionLineageNode}s reflecting what has been executed
     * @param configuration         the execution {@link Configuration}
     * @return the new instance
     */
    public static PartialExecution createFromMeasurement(
            long measuredExecutionTime,
            Collection<ExecutionLineageNode> executionLineageNodes,
            Configuration configuration) {

        // Calculate possible costs.
        double lowerCost = Double.POSITIVE_INFINITY, upperCost = Double.NEGATIVE_INFINITY;
        final Set<Platform> platforms = executionLineageNodes.stream()
                .map(node -> ((ExecutionOperator) node.getOperatorContext().getOperator()).getPlatform())
                .collect(Collectors.toSet());
        for (Platform platform : platforms) {
            final TimeToCostConverter timeToCostConverter = configuration.getTimeToCostConverterProvider().provideFor(platform);
            final ProbabilisticDoubleInterval costs =
                    timeToCostConverter.convertWithoutFixCosts(TimeEstimate.ZERO.plus(measuredExecutionTime));
            lowerCost = Math.min(lowerCost, costs.getLowerEstimate());
            upperCost = Math.max(upperCost, costs.getUpperEstimate());
        }

        return new PartialExecution(measuredExecutionTime, lowerCost, upperCost, executionLineageNodes);
    }

    /**
     * Creates a new instance.
     *
     * @param measuredExecutionTime the time measured for the partial execution
     * @param lowerCost             the lower possible costs for the new instance (excluding fix costs)
     * @param upperCost             the upper possible costs for the new instance (excluding fix costs)
     * @param executionLineageNodes for all executed {@link ExecutionOperator}s
     */
    public PartialExecution(long measuredExecutionTime, double lowerCost, double upperCost, Collection<ExecutionLineageNode> executionLineageNodes) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.atomicExecutionGroups = executionLineageNodes.stream()
                .map(node -> new AtomicExecutionGroup(
                        node.getOperatorContext(),
                        ((ExecutionOperator) node.getOperatorContext().getOperator()).getPlatform(),
                        node.getAtomicExecutions()
                ))
                .collect(Collectors.toList());
        this.lowerCost = lowerCost;
        this.upperCost = upperCost;
        this.involvedPlatforms = executionLineageNodes.stream()
                .map(node -> ((ExecutionOperator) node.getOperatorContext().getOperator()).getPlatform())
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Deserialization constructor.
     */
    private PartialExecution(long measuredExecutionTime, double lowerCost, double upperCost, List<OperatorExecution> executions) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.atomicExecutionGroups = null;
        this.operatorExecutions = executions;
        this.lowerCost = lowerCost;
        this.upperCost = upperCost;
    }

    public long getMeasuredExecutionTime() {
        return measuredExecutionTime;
    }

    /**
     * The lower cost for this instance (without fix costs).
     *
     * @return the lower execution costs
     */
    public double getMeasuredLowerCost() {
        return this.lowerCost;
    }

    /**
     * The upper cost for this instance (without fix costs).
     *
     * @return the upper execution costs
     */
    public double getMeasuredUpperCost() {
        return this.upperCost;
    }

    /**
     * Retrieve the {@link Platform}s involved in this instance.
     *
     * @return the {@link Platform}s
     */
    public Collection<Platform> getInvolvedPlatforms() {
        return this.involvedPlatforms;
    }

    /**
     * Calculates the overall {@link TimeEstimate} of this instance.
     *
     * @return the overall {@link TimeEstimate}
     */
    public TimeEstimate getOverallTimeEstimate(Configuration configuration) {
        final long platformInitializationTime = this.initializedPlatforms.stream()
                .mapToLong(platform -> platform.getInitializeMillis(configuration))
                .sum();
        final TimeEstimate executionTime = this.atomicExecutionGroups.stream()
                .map(group -> group.estimateExecutionTime(configuration))
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        return executionTime.plus(platformInitializationTime);
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
                jsonObject.optLong("lowerCost", -1L), // Default value for backwards compatibility.
                jsonObject.optLong("upperCost", -1L), // Default value for backwards compatibility.
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
            // TODO: Abolish OperatorExecutions.
//            this.operatorExecutions = this.operatorContexts.stream().map(OperatorExecution::new).collect(Collectors.toList());
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
                .put("lowerCost", this.lowerCost)
                .put("upperCost", this.upperCost)
                .put("executions", JsonSerializables.serializeAll(this.getOperatorExecutions()))
                .putOpt("initPlatforms", JsonSerializables.serializeAll(
                        this.getInitializedPlatforms().stream()
                                .map(platform -> platform.getClass().getCanonicalName())
                                .collect(Collectors.toList())
                ));
    }

    /**
     * Provide the {@link AtomicExecutionGroup}s captured by this instance
     *
     * @return the {@link AtomicExecutionGroup}s
     */
    public Collection<AtomicExecutionGroup> getAtomicExecutionGroups() {
        return this.atomicExecutionGroups;
    }

    /**
     * This class groups {@link AtomicExecution}s with a common {@link EstimationContext} and {@link Platform}.
     */
    public static class AtomicExecutionGroup {

        /**
         * The common {@link EstimationContext}.
         */
        private EstimationContext estimationContext;

        /**
         * The common {@link Platform} for all {@link #atomicExecutions}.
         */
        private Platform platform;

        /**
         * The {@link AtomicExecution}s.
         */
        private Collection<AtomicExecution> atomicExecutions;

        public AtomicExecutionGroup(EstimationContext estimationContext,
                                    Platform platform,
                                    Collection<AtomicExecution> atomicExecutions) {
            this.estimationContext = estimationContext;
            this.platform = platform;
            this.atomicExecutions = atomicExecutions;
        }

        /**
         * Estimate the {@link LoadProfile} for all {@link AtomicExecution}s in this instance.
         *
         * @return the {@link LoadProfile}
         */
        public LoadProfile estimateLoad() {
            return this.atomicExecutions.stream()
                    .map(execution -> execution.estimateLoad(this.estimationContext))
                    .reduce(LoadProfile::plus)
                    .orElse(LoadProfile.emptyLoadProfile);
        }

        /**
         * Estimate the {@link TimeEstimate} for all {@link AtomicExecution}s in this instance.
         *
         * @param configuration calibrates the estimation
         * @return the {@link TimeEstimate}
         */
        public TimeEstimate estimateExecutionTime(Configuration configuration) {
            final LoadProfileToTimeConverter converter = configuration
                    .getLoadProfileToTimeConverterProvider()
                    .provideFor(this.platform);
            return converter.convert(this.estimateLoad());
        }

        public EstimationContext getEstimationContext() {
            return this.estimationContext;
        }

        public Platform getPlatform() {
            return this.platform;
        }

        public Collection<AtomicExecution> getAtomicExecutions() {
            return this.atomicExecutions;
        }
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
            return String.format("%s[%s, in=%s, out=%s]",
                    this.getClass().getSimpleName(),
                    this.getOperator(),
                    Arrays.toString(this.getInputCardinalities()),
                    Arrays.toString(this.getOutputCardinalities())
            );
        }
    }
}
