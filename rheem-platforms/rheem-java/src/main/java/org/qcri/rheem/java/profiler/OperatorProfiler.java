package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.Collection;

/**
 * Allows to instrument an {@link JavaExecutionOperator}.
 */
public abstract class OperatorProfiler {

    /**
     * Executes the profiling task. Requires that this instance is prepared.
     */
    public abstract void run();

    protected static ChannelExecutor createChannelExecutor(final Collection<?> collection) {
        final ChannelExecutor channelExecutor = createChannelExecutor();
        channelExecutor.acceptCollection(collection);
        return channelExecutor;
    }

    protected static ChannelExecutor createChannelExecutor() {
        final ChannelDescriptor channelDescriptor = StreamChannel.DESCRIPTOR;
        final StreamChannel streamChannel = new StreamChannel(channelDescriptor, null);
        return JavaPlatform.getInstance().getChannelManager().createChannelExecutor(streamChannel);
    }

    public abstract JavaExecutionOperator getOperator();
}
