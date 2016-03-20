package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * TODO
 */
public abstract class DefaultChannelManager implements ChannelManager {

    private final Platform platform;

    private final ChannelDescriptor reusableInternalChannelDescriptor;

    private final ChannelDescriptor nonreusableInternalChannelDescriptor;

    public DefaultChannelManager(Platform platform,
                                 ChannelDescriptor reusableInternalChannelDescriptor,
                                 ChannelDescriptor nonreusableInternalChannelDescriptor) {
        Validate.notNull(platform);
        this.platform = platform;
        this.reusableInternalChannelDescriptor = reusableInternalChannelDescriptor;
        this.nonreusableInternalChannelDescriptor = nonreusableInternalChannelDescriptor;
    }

    @Override
    public Map<ChannelDescriptor, Channel> setUpSourceSide(
            Junction junction,
            List<ChannelDescriptor> preferredChannelDescriptors,
            OptimizationContext optimizationContext) {

        // Find out if we need a reusable internal Channel at first. This is the case if we have multiple consumers.
        final ChannelManager sourceChannelManager = junction.getSourceChannelManager();
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

        // Pick the internal ChannelDescriptors.
        final List<ChannelDescriptor> supportedOutputChannels = junction.getSourceOperator()
                .getSupportedOutputChannels(junction.getSourceOutput().getIndex());
        final List<ChannelDescriptor> internalChannelDescriptors = new ArrayList<>(junction.getTargetInputs().size());
        ChannelDescriptor internalChannelDescriptor = null;
        for (InputSlot<?> inputSlot : junction.getTargetInputs()) {
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
        OptimizationContext forkOptimizationCtx = new OptimizationContext(optimizationContext);
        final ChannelInitializer internalChannelInitializer = sourceChannelManager.getChannelInitializer(internalChannelDescriptor);
        final Tuple<Channel, Channel> internalSourceChannelSetup = internalChannelInitializer.setUpOutput(
                internalChannelDescriptor, junction.getSourceOutput(), forkOptimizationCtx);
        junction.setSourceChannel(internalSourceChannelSetup.field0);
        final Channel outboundInternalSourceChannel = internalSourceChannelSetup.field1;

        // Set up the internal consumers.
        for (int targetIndex = 0; targetIndex < internalChannelDescriptors.size(); targetIndex++) {
            if (internalChannelDescriptors.get(targetIndex) != null) {
                assert internalChannelDescriptors.get(targetIndex).equals(outboundInternalSourceChannel.getDescriptor());
                junction.setTargetChannel(targetIndex, outboundInternalSourceChannel);
            }
        }

        // Set up all external Channels.
        Map<ChannelDescriptor, Channel> externalChannels = new HashMap<>(2);
        for (int targetIndex = 0; targetIndex < junction.getTargetChannels().size(); targetIndex++) {
            final ChannelDescriptor extChannelDescriptor = preferredChannelDescriptors.get(targetIndex);
            if (extChannelDescriptor.isInternal()) continue;

            // Get or set up the Channel.
            externalChannels.computeIfAbsent(extChannelDescriptor, cd -> {
                final ChannelInitializer channelInitializer = sourceChannelManager.getChannelInitializer(cd);
                return channelInitializer.setUpOutput(cd, outboundInternalSourceChannel, forkOptimizationCtx);
            });
        }

        return externalChannels;
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


    public ChannelDescriptor getInternalChannelDescriptor(boolean isRequestReusable) {
        return isRequestReusable ?
                this.reusableInternalChannelDescriptor :
                this.nonreusableInternalChannelDescriptor;
    }

    @Override
    public void setUpTargetSide(Junction junction, int targetIndex, Channel externalChannel, OptimizationContext optimizationContext) {

        // Set up an internal Channel for the target InputSlot.
        final InputSlot<?> targetInput = junction.getTargetInput(targetIndex);
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
        final Channel internalTargetChannel = targetChannelInitializer.setUpOutput(
                internalTargetChannelDescriptor, externalChannel, optimizationContext);
        junction.setTargetChannel(targetIndex, internalTargetChannel);
    }

    @Override
    public boolean exchangeWithInterstageCapable(Channel channel) {
        return false;
    }

}
