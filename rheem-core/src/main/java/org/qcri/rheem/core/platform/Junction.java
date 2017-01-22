package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Describes the implementation of one {@link OutputSlot} to its occupied {@link InputSlot}s.
 */
public class Junction {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(Junction.class);

    private final OutputSlot<?> sourceOutput;

    private Channel sourceChannel;

    private final List<InputSlot<?>> targetInputs;

    private final List<Channel> targetChannels;

    private final List<OptimizationContext> optimizationContexts;

    private final Collection<ExecutionTask> conversionTasks = new LinkedList<>();

    private TimeEstimate timeEstimateCache = null;

    public Junction(OutputSlot<?> sourceOutput, List<InputSlot<?>> targetInputs, List<OptimizationContext> optimizationContexts) {
        // Copy parameters.
        assert sourceOutput.getOwner().isExecutionOperator();
        this.sourceOutput = sourceOutput;
        assert targetInputs.stream().allMatch(input -> input.getOwner().isExecutionOperator());
        this.targetInputs = targetInputs;

        // Fill with nulls.
        this.targetChannels = RheemCollections.map(this.targetInputs, input -> null);

        // Get hold of an OptimizationContext.
        this.optimizationContexts = optimizationContexts;
    }

    public ExecutionOperator getSourceOperator() {
        return (ExecutionOperator) this.sourceOutput.getOwner();
    }

    public ExecutionOperator getTargetOperator(int targetIndex) {
        return (ExecutionOperator) this.getTargetInputs().get(targetIndex).getOwner();
    }

    public OutputSlot<?> getSourceOutput() {
        return this.sourceOutput;
    }

    @SuppressWarnings("unchecked")
    public Collection<OutputSlot<?>> getOuterSourceOutputs() {
        return (Collection) this.getSourceOperator().getOutermostOutputSlots(this.getSourceOutput());
    }

    public List<InputSlot<?>> getTargetInputs() {
        return this.targetInputs;
    }

    public InputSlot<?> getTargetInput(int targetIndex) {
        return this.getTargetInputs().get(targetIndex);
    }

    public Channel getSourceChannel() {
        return this.sourceChannel;
    }

    public void setSourceChannel(Channel sourceChannel) {
        this.sourceChannel = sourceChannel;
    }

    public List<Channel> getTargetChannels() {
        return this.targetChannels;
    }

    public Channel getTargetChannel(int targetIndex) {
        return this.targetChannels.get(targetIndex);
    }

    public void setTargetChannel(int targetIndex, Channel targetChannel) {
        assert this.targetChannels.get(targetIndex) == null : String.format(
                "Cannot set target channel %d to %s; it is already occupied by %s.",
                targetIndex, targetChannel, this.targetChannels.get(targetIndex)
        );
        this.targetChannels.set(targetIndex, targetChannel);
    }

    public int getNumTargets() {
        return this.targetInputs.size();
    }

    public Collection<ExecutionTask> getConversionTasks() {
        return this.conversionTasks;
    }

    /**
     * Calculates the {@link TimeEstimate} for all {@link ExecutionOperator}s in this instance for a given
     * {@link OptimizationContext} that should be known itself (or as a fork) to this instance.
     *
     * @param optimizationContext the {@link OptimizationContext}
     * @return the aggregate {@link TimeEstimate}
     */
    public TimeEstimate getTimeEstimate(OptimizationContext optimizationContext) {
        final OptimizationContext localMatchingOptCtx = this.findMatchingOptimizationContext(optimizationContext);
        assert localMatchingOptCtx != null : "No matching OptimizationContext for in Junction.";
        return this.conversionTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(localMatchingOptCtx::getOperatorContext)
                .map(OptimizationContext.OperatorContext::getTimeEstimate)
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
    }

    /**
     * Calculates the cost estimate for all {@link ExecutionOperator}s in this instance for a given
     * {@link OptimizationContext} that should be known itself (or as a fork) to this instance.
     *
     * @param optimizationContext the {@link OptimizationContext}
     * @return the aggregate cost estimate
     */
    public ProbabilisticDoubleInterval getCostEstimate(OptimizationContext optimizationContext) {
        final OptimizationContext localMatchingOptCtx = this.findMatchingOptimizationContext(optimizationContext);
        assert localMatchingOptCtx != null : "No matching OptimizationContext for in Junction.";
        return this.conversionTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(localMatchingOptCtx::getOperatorContext)
                .map(OptimizationContext.OperatorContext::getCostEstimate)
                .reduce(ProbabilisticDoubleInterval.zero, ProbabilisticDoubleInterval::plus);
    }

    /**
     * Calculates the cost estimate for all {@link ExecutionOperator}s in this instance for a given
     * {@link OptimizationContext} that should be known itself (or as a fork) to this instance.
     *
     * @param optimizationContext the {@link OptimizationContext}
     * @return the aggregate cost estimate
     */
    public double getSquashedCostEstimate(OptimizationContext optimizationContext) {
        final OptimizationContext localMatchingOptCtx = this.findMatchingOptimizationContext(optimizationContext);
        assert localMatchingOptCtx != null : "No matching OptimizationContext for in Junction.";
        return this.conversionTasks.stream()
                .map(ExecutionTask::getOperator)
                .map(localMatchingOptCtx::getOperatorContext)
                .mapToDouble(OptimizationContext.OperatorContext::getSquashedCostEstimate)
                .sum();
    }

    /**
     * Determines a matching {@link OptimizationContext} from {@link #optimizationContexts} w.r.t. the given
     * {@link OptimizationContext}. A match is given if the local {@link OptimizationContext} is either forked
     * from {@code externalOptCtx} or a parent.
     *
     * @param externalOptCtx the non-local {@link OptimizationContext}
     * @return the local matching {@link OptimizationContext}
     */
    private OptimizationContext findMatchingOptimizationContext(OptimizationContext externalOptCtx) {
        for (OptimizationContext optCtx : this.optimizationContexts) {
            if (optCtx == externalOptCtx || optCtx.getBase() == externalOptCtx) {
                return optCtx;
            }
        }

        if (externalOptCtx.getParent() != null) {
            return this.findMatchingOptimizationContext(externalOptCtx.getParent());
        }

        return null;
    }

    /**
     * Determines the {@link TimeEstimate} for all {@link ExecutionOperator}s in this instance across all of its
     * {@link OptimizationContext}s.
     *
     * @return the aggregate {@link TimeEstimate}
     */
    public TimeEstimate getOverallTimeEstimate() {
        if (this.timeEstimateCache == null) {
            this.timeEstimateCache = this.optimizationContexts.stream()
                    .map(this::getTimeEstimate)
                    .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
        }
        return this.timeEstimateCache;
    }


    @Override
    public String toString() {
        return String.format("%s[%s->%s]", this.getClass().getSimpleName(), this.getSourceOutput(), this.getTargetInputs());
    }

    /**
     * Registers an {@link ExecutionTask} that participates in the instance. All such {@link ExecutionTask}s must
     * be registered to provide proper estimates.
     *
     * @param conversionTask the {@link ExecutionTask}
     */
    public void register(ExecutionTask conversionTask) {
        this.conversionTasks.add(conversionTask);
        this.timeEstimateCache = null;
    }

    /**
     * Retrieve the {@link OptimizationContext}s that hold optimization information on the {@link ExecutionOperator}s
     * in this instance.
     *
     * @return the {@link OptimizationContext}s
     */
    public List<OptimizationContext> getOptimizationContexts() {
        return this.optimizationContexts;
    }

}
