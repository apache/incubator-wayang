package org.qcri.rheem.core.platform;

import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Captures data of a execution of a set of {@link ExecutionOperator}s.
 */
public class PartialExecution {

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

        return new PartialExecution(measuredExecutionTime, lowerCost, upperCost, executionLineageNodes, configuration);
    }

    /**
     * Creates a new instance.
     *
     * @param measuredExecutionTime the time measured for the partial execution
     * @param lowerCost             the lower possible costs for the new instance (excluding fix costs)
     * @param upperCost             the upper possible costs for the new instance (excluding fix costs)
     * @param executionLineageNodes for all executed {@link ExecutionOperator}s
     * @param configuration         the {@link Configuration} to re-estimate execution statistics
     */
    public PartialExecution(long measuredExecutionTime, double lowerCost, double upperCost,
                            Collection<ExecutionLineageNode> executionLineageNodes,
                            Configuration configuration) {
        this.measuredExecutionTime = measuredExecutionTime;
        this.atomicExecutionGroups = executionLineageNodes.stream()
                .map(node -> new AtomicExecutionGroup(
                        node.getOperatorContext(),
                        ((ExecutionOperator) node.getOperatorContext().getOperator()).getPlatform(),
                        configuration,
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
                .map(group -> group.estimateExecutionTime())
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
     * Provide the {@link AtomicExecutionGroup}s captured by this instance
     *
     * @return the {@link AtomicExecutionGroup}s
     */
    public Collection<AtomicExecutionGroup> getAtomicExecutionGroups() {
        return this.atomicExecutionGroups;
    }

    /**
     * {@link JsonSerializer} implementation for {@link PartialExecution}s.
     */
    public static class Serializer implements JsonSerializer<PartialExecution> {

        private final Configuration configuration;

        /**
         * Creates a new instance.
         *
         * @param configuration is required for deserialization; can otherwise be {@code null}
         */
        public Serializer(Configuration configuration) {
            this.configuration = configuration;
        }


        @Override
        public JSONObject serialize(PartialExecution pe) {
            return new JSONObject()
                    .put("millis", pe.measuredExecutionTime)
                    .put("lowerCost", pe.lowerCost)
                    .put("upperCost", pe.upperCost)
                    .put("execGroups", JsonSerializables.serializeAll(
                            pe.atomicExecutionGroups,
                            false,
                            new AtomicExecutionGroup.Serializer(this.configuration))
                    )
                    .putOpt(
                            "initPlatforms",
                            JsonSerializables.serializeAll(pe.initializedPlatforms, true, Platform.jsonSerializer)
                    );
        }

        @Override
        public PartialExecution deserialize(JSONObject json, Class<? extends PartialExecution> cls) {
            final long measuredExecutionTime = json.getLong("millis");
            final double lowerCost = json.optDouble("lowerCost", -1);
            final double uppserCost = json.optDouble("upperCost", -1);
            final Collection<AtomicExecutionGroup> atomicExecutionGroups =
                    JsonSerializables.deserializeAllAsList(
                            json.getJSONArray("execGroups"),
                            new AtomicExecutionGroup.Serializer(this.configuration),
                            AtomicExecutionGroup.class
                    );
            final Collection<Platform> initializedPlatforms =
                    JsonSerializables.deserializeAllAsList(json.optJSONArray("initPlatforms"), Platform.jsonSerializer);
            final PartialExecution partialExecution = new PartialExecution(
                    atomicExecutionGroups, measuredExecutionTime, lowerCost, uppserCost
            );
            partialExecution.initializedPlatforms.addAll(initializedPlatforms);
            return partialExecution;
        }
    }


}
