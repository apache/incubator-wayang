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
import java.util.function.Predicate;

/**
 * Describes the implementation of one {@link OutputSlot} to its occupied {@link InputSlot}s.
 */
public class Junction {

    private static final Logger logger = LoggerFactory.getLogger(Junction.class);

    private final OutputSlot<?> sourceOutput;

    private Channel sourceChannel;

    private final List<InputSlot<?>> targetInputs;

    private final List<Channel> targetChannels;

    private final OptimizationContext localOptimizationContext;

    private TimeEstimate timeEstimateCache;

    public Junction(OutputSlot<?> sourceOutput, List<InputSlot<?>> targetInputs, OptimizationContext baseOptimizationCtx) {
        // Copy parameters.
        assert sourceOutput.getOwner().isExecutionOperator();
        this.sourceOutput = sourceOutput;
        assert targetInputs.stream().allMatch(input -> input.getOwner().isExecutionOperator());
        this.targetInputs = targetInputs;
        this.localOptimizationContext = new OptimizationContext(baseOptimizationCtx);

        // Fill with nulls.
        this.targetChannels = RheemCollections.map(this.targetInputs, input -> null);
    }

    public ExecutionOperator getSourceOperator() {
        return (ExecutionOperator) this.sourceOutput.getOwner();
    }

    public ExecutionOperator getTargetOperator(int targetIndex) {
        return (ExecutionOperator) this.getTargetInputs().get(targetIndex).getOwner();
    }

    /**
     * Picks a {@link Channel} class that exists in both given {@link List}s.
     *
     * @param supportedOutputChannels a {@link List} of (output) {@link Channel} classes
     * @param supportedInputChannels  a {@link List} of (input) {@link Channel} classes
     * @return the picked {@link Channel} class or {@code null} if none was picked
     */
    protected ChannelDescriptor pickChannelDescriptor(List<ChannelDescriptor> supportedOutputChannels,
                                                      List<ChannelDescriptor> supportedInputChannels) {
        return this.pickChannelDescriptor(supportedOutputChannels, supportedInputChannels, cd -> true);
    }

    /**
     * Picks a {@link Channel} class that exists in both given {@link List}s.
     *
     * @param supportedOutputChannels a {@link List} of (output) {@link Channel} classes
     * @param supportedInputChannels  a {@link List} of (input) {@link Channel} classes
     * @param filter                  criterion on which {@link Channel}s are allowed
     * @return the picked {@link Channel} class or {@code null} if none was picked
     */
    protected ChannelDescriptor pickChannelDescriptor(List<ChannelDescriptor> supportedOutputChannels,
                                                      List<ChannelDescriptor> supportedInputChannels,
                                                      Predicate<ChannelDescriptor> filter) {
        for (ChannelDescriptor supportedOutputChannel : supportedOutputChannels) {
            if (!filter.test(supportedOutputChannel)) continue;
            if (supportedInputChannels.contains(supportedOutputChannel)) {
                return supportedOutputChannel;
            }
        }
        return null;
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
        assert this.targetChannels.get(targetIndex) == null;
        this.targetChannels.set(targetIndex, targetChannel);
    }

    public int getNumTargets() {
        return this.targetInputs.size();
    }

    public TimeEstimate getTimeEstimate() {
        if (this.timeEstimateCache == null) {
            this.timeEstimateCache = this.createTimeEstimate();
        }
        return this.timeEstimateCache;
    }

    private TimeEstimate createTimeEstimate() {
        return this.localOptimizationContext.getLocalOperatorContexts().values().stream()
                .map(OptimizationContext.OperatorContext::getTimeEstimate)
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
    }

    @Override
    public String toString() {
        return String.format("%s[%s->%s]", this.getClass().getSimpleName(), this.getSourceOutput(), this.getTargetInputs());
    }
}
