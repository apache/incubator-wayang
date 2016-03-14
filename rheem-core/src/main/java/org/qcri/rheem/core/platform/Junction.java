package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Describes the implementation of one {@link OutputSlot} to its occupied {@link InputSlot}s.
 */
public class Junction {

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

    /**
     * Algorithm to negotiate about best {@link Channel}s between the {@link #sourceOutput} and the
     * {@link #targetInputs}. The below describes the interplay with a {@link DefaultChannelManager}.
     * <p>
     * <p>We assume a 1/3 layer-model is the solution to this problem:
     * <ul>
     * <li>internal {@link Channel} only if available</li>
     * <li>internal->external->internal {@link Channel} if only external {@link Channel}s possible</li>
     * </ul>
     * </p>
     * <p>
     * <p>The negotiation algorithm works as follows:
     * <ol>
     * <li>Negotiate on best {@link Channel} for each {@link InputSlot}.</li>
     * <li>Find out if we need a reusable internal {@link Channel} on the source side.</li>
     * <li>Set up this internal {@link Channel} and register with appropriate {@link #targetInputs}.</li>
     * <li>Set up each required external {@link Channel} based on the internal {@link Channel}.</li>
     * <li>Set up internal target {@link Channel}s based on the external {@link Channel}.</li>
     * </ol>
     * </p>
     * <p>
     * NB: We do not merge external consumers with the same Platform. We could in fact set up a single internal
     * Channel for them, but how do we know, if they will belong to the same PlatformExecution?
     * </p>
     *
     * @return whether the setup was successful
     */
    public boolean setUp() {
        // Find the best-matching ChannelDescriptor for each InputSlot.
        List<ChannelDescriptor> preferredChannelDescriptors = this.getPreferredChannelDescriptors();
        if (preferredChannelDescriptors == null) return false;

        // Set up the "source side" of the junction.
        final ChannelManager sourceChannelManager = this.getSourceChannelManager();
        final Map<ChannelDescriptor, Channel> externalChannels =
                sourceChannelManager.setUpSourceSide(this, preferredChannelDescriptors, this.localOptimizationContext);
        if (externalChannels == null) return false;

        // Set up remaining connections that go via external Channels.
        for (int targetIndex = 0; targetIndex < this.targetChannels.size(); targetIndex++) {
            final ChannelDescriptor extChannelDescriptor = preferredChannelDescriptors.get(targetIndex);
            if (extChannelDescriptor.isInternal()) continue;

            final Channel externalChannel = externalChannels.get(extChannelDescriptor);
            assert externalChannel != null;
            final ChannelManager targetChannelManager = this.getTargetChannelManager(targetIndex);
            targetChannelManager.setUpTargetSide(this, targetIndex, externalChannel, this.localOptimizationContext);
        }

        // TODO: We could do a post-processing of the plan, once we have settled the PlatformExecution.

        return true;
    }

    /**
     * Finds the preferred {@link ChannelDescriptor} between the {@link #sourceOutput} and each {@link #targetInputs}
     * or {@code null} if not all {@link #targetInputs} can be connected.
     */
    private List<ChannelDescriptor> getPreferredChannelDescriptors() {
        final List<ChannelDescriptor> supportedOutputChannels = this.getSourceOperator()
                .getSupportedOutputChannels(this.sourceOutput.getIndex());
        List<ChannelDescriptor> preferredChannelDescriptors = new ArrayList<>(this.targetInputs.size());
        for (InputSlot<?> inputSlot : this.targetInputs) {
            final ExecutionOperator targetOperator = (ExecutionOperator) inputSlot.getOwner();
            final List<ChannelDescriptor> supportedInputChannels = targetOperator.getSupportedInputChannels(inputSlot.getIndex());

            ChannelDescriptor preferredChannelDescriptor = this.pickChannelDescriptor(supportedOutputChannels, supportedInputChannels);
            if (preferredChannelDescriptor == null) {
                return null;
            } else {
                preferredChannelDescriptors.add(preferredChannelDescriptor);
            }
        }
        return preferredChannelDescriptors;
    }

    public ChannelManager getSourceChannelManager() {
        return ((ExecutionOperator) this.sourceOutput.getOwner()).getPlatform().getChannelManager();
    }

    public ExecutionOperator getSourceOperator() {
        return (ExecutionOperator) this.sourceOutput.getOwner();
    }

    public ChannelManager getTargetChannelManager(int targetIndex) {
        return this.getTargetOperator(targetIndex).getPlatform().getChannelManager();
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

    public static Junction create(OutputSlot<?> outputSlot,
                                  List<InputSlot<?>> inputSlots,
                                  OptimizationContext baseOptimizationCtx) {
        final Junction junction = new Junction(outputSlot, inputSlots, baseOptimizationCtx);
        return junction.setUp() ? junction : null;
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
