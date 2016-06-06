package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemCollections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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

    private TimeEstimate timeEstimateCache = TimeEstimate.ZERO;

    public Junction(OutputSlot<?> sourceOutput, List<InputSlot<?>> targetInputs, OptimizationContext baseOptimizationCtx) {
        // Copy parameters.
        assert sourceOutput.getOwner().isExecutionOperator();
        this.sourceOutput = sourceOutput;
        assert targetInputs.stream().allMatch(input -> input.getOwner().isExecutionOperator());
        this.targetInputs = targetInputs;

        // Fill with nulls.
        this.targetChannels = RheemCollections.map(this.targetInputs, input -> null);
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

    public TimeEstimate getTimeEstimate() {
        return this.timeEstimateCache;
    }


    @Override
    public String toString() {
        return String.format("%s[%s->%s]", this.getClass().getSimpleName(), this.getSourceOutput(), this.getTargetInputs());
    }

    public void register(ExecutionOperator conversionOperator, TimeEstimate costs) {
        this.timeEstimateCache = this.timeEstimateCache.plus(costs);
    }
}
