package org.qcri.rheem.graphchi.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.graphchi.GraphChiPlatform;

/**
 * Helpers for dealing with {@link FileChannel}s on the {@link GraphChiPlatform}.
 */
public class FileChannels {

    /**
     * Initializes {@link FileChannel}s for use with the {@link GraphChiPlatform}.
     */
    public static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ChannelDescriptor channelDescriptor, ExecutionTask executionTask, int index) {
            final Channel existingOutputChannel = executionTask.getOutputChannel(index);
            if (existingOutputChannel == null) {
                final FileChannel fileChannel = new FileChannel((FileChannel.Descriptor) channelDescriptor,
                        executionTask,
                        index,
                        executionTask.getOperator().getOutput(index).getCardinalityEstimate());
                fileChannel.addPath(FileChannel.pickTempPath());
                return fileChannel;
            } else if (existingOutputChannel instanceof FileChannel) {
                // TODO: To be on the safe side, we might check if this FileChannel fits the channelDescriptor.
                return existingOutputChannel;
            } else {
                throw new IllegalStateException();
            }
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            channel.addConsumer(executionTask, index);
        }

    }

}
