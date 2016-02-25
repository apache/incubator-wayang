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
public class HdfsFiles {

    /**
     * Initializes {@link FileChannel}s for use with the {@link GraphChiPlatform}.
     */
    public static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ChannelDescriptor channelDescriptor, ExecutionTask executionTask, int index) {
            throw new RuntimeException("Implement me."); // TODO
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {
            channel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return true;
        }

        @Override
        public boolean isInternal() {
            return false;
        }
    }

}
