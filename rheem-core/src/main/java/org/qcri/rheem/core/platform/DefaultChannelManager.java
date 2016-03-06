package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.Tuple;

import java.util.ArrayList;
import java.util.List;

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

    /**
     * Picks a {@link Channel} class that exists in both given {@link List}s.
     *
     * @param supportedOutputChannels a {@link List} of (output) {@link Channel} classes
     * @param supportedInputChannels  a {@link List} of (input) {@link Channel} classes
     * @return the picked {@link Channel} class or {@code null} if none was picked
     */
    protected ChannelDescriptor pickChannelDescriptor(List<ChannelDescriptor> supportedOutputChannels,
                                                      List<ChannelDescriptor> supportedInputChannels) {
        for (ChannelDescriptor supportedOutputChannel : supportedOutputChannels) {
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
    public boolean exchangeWithInterstageCapable(Channel channel) {
        return false;
    }

}
