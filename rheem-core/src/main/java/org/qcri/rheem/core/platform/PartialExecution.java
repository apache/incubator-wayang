package org.qcri.rheem.core.platform;

import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.JsonSerializables;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
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
    private final Collection<AtomicExecutionGroup> atomicExecutionGroups;

    /**
     * Platforms initialized/involved this instance.
     */
    private Collection<Platform> initializedPlatforms = new LinkedList<>();

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
    }

    /**
     * Deserialization constructor.
     */
    private PartialExecution(Collection<AtomicExecutionGroup> atomicExecutionGroups,
                             long measuredExecutionTime,
                             double lowerCost,
                             double upperCost) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.atomicExecutionGroups = atomicExecutionGroups;
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
     * <i>Note that this method can only be successful if the {@link AtomicExecutionGroup}s use
     * {@link OptimizationContext.OperatorContext}s as their {@link EstimationContext}.</i>
     *
     * @return the {@link Platform}s
     */
    public Set<Platform> getInvolvedPlatforms() {
        return this.atomicExecutionGroups.stream().map(AtomicExecutionGroup::getPlatform).collect(Collectors.toSet());
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
                .put("execGroups", JsonSerializables.serializeAll(this.atomicExecutionGroups, false))
                .putOpt("initPlatforms", JsonSerializables.serializeAll(this.initializedPlatforms, true, Platform.jsonSerializer));
    }

    /**
     * Parses a {@link JSONObject} and creates a new instance.
     *
     * @param jsonObject to be parsed
     * @return the new instance
     */
    public static PartialExecution fromJson(JSONObject jsonObject) {
        final long measuredExecutionTime = jsonObject.getLong("millis");
        final double lowerCost = jsonObject.optDouble("lowerCost", -1);
        final double uppserCost = jsonObject.optDouble("upperCost", -1);
        final Collection<PartialExecution.AtomicExecutionGroup> atomicExecutionGroups =
                JsonSerializables.deserializeAllAsList(jsonObject.getJSONArray("execGroups"), AtomicExecutionGroup.class);
        final Collection<Platform> initializedPlatforms =
                JsonSerializables.deserializeAllAsList(jsonObject.optJSONArray("initPlatforms"), Platform.jsonSerializer);
        final PartialExecution partialExecution = new PartialExecution(
                atomicExecutionGroups, measuredExecutionTime, lowerCost, uppserCost
        );
        partialExecution.initializedPlatforms.addAll(initializedPlatforms);
        return partialExecution;

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
    public static class AtomicExecutionGroup implements JsonSerializable {

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

        @Override
        public JSONObject toJson() {
            return new JSONObject()
                    .put("ctx", JsonSerializables.serialize(this.estimationContext, true))
                    .put("platform", JsonSerializables.serialize(this.platform, true, Platform.jsonSerializer))
                    .put("executions", JsonSerializables.serializeAll(this.atomicExecutions, false));

        }
    }

}
