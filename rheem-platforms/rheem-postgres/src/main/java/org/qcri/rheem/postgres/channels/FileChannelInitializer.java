package org.qcri.rheem.postgres.channels;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.postgres.PostgresPlatform;

 /**
 * Initializes {@link FileChannel}s for use with the {@link PostgresPlatform}.
 */
public class FileChannelInitializer implements ChannelInitializer {


        @Override
        public Tuple<Channel, Channel> setUpOutput(ChannelDescriptor descriptor, OutputSlot<?> outputSlot, OptimizationContext optimizationContext) {
            final FileChannel fileChannel = new FileChannel((FileChannel.Descriptor) descriptor, outputSlot);
            fileChannel.addPath(FileChannel.pickTempPath());
            return new Tuple<>(fileChannel, fileChannel);
        }

        @Override
        public Channel setUpOutput(ChannelDescriptor descriptor, Channel source, OptimizationContext optimizationContext) {
            throw new UnsupportedOperationException("Not (yet) implemented.");
        }

}
