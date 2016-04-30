package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.channels.PostgresInternalChannel;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public interface PostgresExecutionOperator extends ExecutionOperator {

    String evaluate(ChannelInstance[] inputChannels, ChannelInstance[] outputChannels, FunctionCompiler compiler);

    @Override
    default Platform getPlatform() {
        return PostgresPlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(PostgresInternalChannel.DESCRIPTOR);
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        List<ChannelDescriptor> supportedChannels = new LinkedList<>();
        supportedChannels.add(PostgresInternalChannel.DESCRIPTOR);
        supportedChannels.add(FileChannel.HDFS_TSV_DESCRIPTOR);
        return supportedChannels;
    }

}