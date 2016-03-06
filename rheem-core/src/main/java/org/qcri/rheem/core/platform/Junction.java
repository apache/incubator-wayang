package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
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

    public Junction(OutputSlot<?> sourceOutput, List<InputSlot<?>> targetInputs) {
        this.sourceOutput = sourceOutput;
        this.targetInputs = targetInputs;
        this.targetChannels = RheemCollections.map(this.targetInputs, input -> null);
    }

    /**
     * Algorithm to negotiate about best {@link Channel}s between the {@link #sourceOutput} and the
     * {@link #targetInputs}.
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
        // Gather all supported output ChannelDescriptors.
        final ExecutionOperator sourceOperator = (ExecutionOperator) this.sourceOutput.getOwner();
        final List<ChannelDescriptor> supportedOutputChannels = sourceOperator
                .getSupportedOutputChannels(this.sourceOutput.getIndex());

        // Find the best-matching ChannelDescriptor for each InputSlot.
        List<ChannelDescriptor> preferredChannelDescriptors = new ArrayList<>(this.targetInputs.size());
        for (InputSlot<?> inputSlot : this.targetInputs) {
            final ExecutionOperator targetOperator = (ExecutionOperator) inputSlot.getOwner();
            final List<ChannelDescriptor> supportedInputChannels = targetOperator.getSupportedInputChannels(inputSlot.getIndex());

            ChannelDescriptor preferredChannelDescriptor = this.pickChannelDescriptor(supportedOutputChannels, supportedInputChannels);
            if (preferredChannelDescriptor == null) {
                return false;
            } else {
                preferredChannelDescriptors.add(preferredChannelDescriptor);
            }
        }

        // Find out if we need a reusable internal Channel at first. This is the case if we have multiple consumers.
        final ChannelManager sourceChannelManager = this.getSourceChannelManager();
        int numDistinctExternalChannels = (int) preferredChannelDescriptors.stream()
                .filter(descriptor -> !descriptor.isInternal())
                .distinct()
                .count();
        int numInternalChannels = (int) preferredChannelDescriptors.stream()
                .filter(ChannelDescriptor::isInternal)
                .count();
        boolean hasInternalReusableChannel = preferredChannelDescriptors.stream()
                .allMatch(descriptor -> descriptor.isInternal() && descriptor.isInternal());
        boolean isRequestReusableInternalChannel = hasInternalReusableChannel ||
                (numDistinctExternalChannels + numInternalChannels) > 1;

        // Set up the internal source Channel.
        final List<ChannelDescriptor> internalChannelDescriptors = new ArrayList<>(this.targetInputs.size());
        ChannelDescriptor internalChannelDescriptor = null;
        for (InputSlot<?> inputSlot : this.targetInputs) {
            final ExecutionOperator targetOperator = (ExecutionOperator) inputSlot.getOwner();
            final List<ChannelDescriptor> supportedInputChannels = targetOperator.getSupportedInputChannels(inputSlot.getIndex());

            ChannelDescriptor preferredInternalChannelDescriptor = this.pickChannelDescriptor(
                    supportedOutputChannels,
                    supportedInputChannels,
                    (channelDescriptor) -> channelDescriptor.isInternal()
                            && (!isRequestReusableInternalChannel || channelDescriptor.isReusable())
            );
            if (preferredInternalChannelDescriptor != null) {
                assert internalChannelDescriptor == null || preferredInternalChannelDescriptor.equals(internalChannelDescriptor)
                        : String.format("Assumed agreement on internal Channel, got %s and %s.",
                        internalChannelDescriptor, preferredInternalChannelDescriptor);
                internalChannelDescriptor = preferredInternalChannelDescriptor;
            }
            internalChannelDescriptors.add(preferredInternalChannelDescriptor);
        }

        // Fallback: there was no pick of an internal Channel.
        if (internalChannelDescriptor == null) {
            internalChannelDescriptor = sourceChannelManager.getInternalChannelDescriptor(isRequestReusableInternalChannel);
        }

        // Set up the internal Channel.
        final ChannelInitializer internalChannelInitializer = sourceChannelManager.getChannelInitializer(internalChannelDescriptor);
        final Tuple<Channel, Channel> internalSourceChannelSetup = internalChannelInitializer.setUpOutput(internalChannelDescriptor, this.sourceOutput);
        this.sourceChannel = internalSourceChannelSetup.field0;
        final Channel outboundInternalSourceChannel = internalSourceChannelSetup.field1;

        // Set up the internal consumers.
        boolean isAllChannelsInternal = true;
        for (int targetIndex = 0; targetIndex < internalChannelDescriptors.size(); targetIndex++) {
            if (internalChannelDescriptors.get(targetIndex) != null) {
                assert internalChannelDescriptors.get(targetIndex).equals(outboundInternalSourceChannel.getDescriptor());
                this.targetChannels.set(targetIndex, outboundInternalSourceChannel);
            } else {
                isAllChannelsInternal = false;
            }
        }
        if (isAllChannelsInternal) return true;

        // Set up all external Channels.
        Map<ChannelDescriptor, Channel> externalChannels = new HashMap<>(2);
        for (int targetIndex = 0; targetIndex < internalChannelDescriptors.size(); targetIndex++) {
            final ChannelDescriptor extChannelDescriptor = preferredChannelDescriptors.get(targetIndex);
            if (extChannelDescriptor.isInternal()) continue;

            // Get or set up the Channel.
            final Channel externalChannel = externalChannels.computeIfAbsent(extChannelDescriptor, cd -> {
                final ChannelInitializer channelInitializer = sourceChannelManager.getChannelInitializer(cd);
                return channelInitializer.setUpOutput(cd, outboundInternalSourceChannel);
            });

            // Set up an internal Channel for the target InputSlot.
            final InputSlot<?> targetInput = this.targetInputs.get(targetIndex);
            final ExecutionOperator targetOperator = (ExecutionOperator) targetInput.getOwner();
            final ChannelDescriptor internalTargetChannelDescriptor = targetOperator
                    .getSupportedInputChannels(targetInput.getIndex()).stream()
                    .filter(ChannelDescriptor::isInternal)
                    .findFirst()
                    .orElseThrow(() ->
                            new RheemException(String.format("No available internal channel for %s.", targetOperator))
                    );

            final ChannelManager targetChannelManager = targetOperator.getPlatform().getChannelManager();
            final ChannelInitializer targetChannelInitializer =
                    targetChannelManager.getChannelInitializer(internalTargetChannelDescriptor);
            final Channel internalTargetChannel = targetChannelInitializer.setUpOutput(internalTargetChannelDescriptor, externalChannel);
            this.targetChannels.set(targetIndex, internalTargetChannel);
        }

        // TODO: We could do a post-processing of the plan, once we have settled the PlatformExecution.

        return true;
    }

    private ChannelManager getSourceChannelManager() {
        return ((ExecutionOperator) this.sourceOutput.getOwner()).getPlatform().getChannelManager();
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

    public static Junction create(OutputSlot<?> outputSlot, List<InputSlot<?>> inputSlots) {
        final Junction junction = new Junction(outputSlot, inputSlots);
        return junction.setUp() ? junction : null;
    }

    public Channel getSourceChannel() {
        return this.sourceChannel;
    }

    public List<Channel> getTargetChannels() {
        return this.targetChannels;
    }

    public OutputSlot<?> getOutput() {
        return this.sourceOutput;
    }

    public List<InputSlot<?>> getInputs() {
        return this.targetInputs;
    }
}
