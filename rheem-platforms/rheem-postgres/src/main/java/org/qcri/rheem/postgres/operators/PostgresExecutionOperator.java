package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.channels.PostgresChannelManager;
import org.qcri.rheem.postgres.compiler.FunctionCompiler;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public interface PostgresExecutionOperator extends ExecutionOperator {

    String evaluate(Channel[] inputChannels, Channel[] outputChannels, FunctionCompiler compiler);


    @Override
    default Platform getPlatform() {
        return PostgresPlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(PostgresChannelManager.INTERNAL_DESCRIPTOR);
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        List<ChannelDescriptor> supportedChannels = new LinkedList<>();
        supportedChannels.add(PostgresChannelManager.INTERNAL_DESCRIPTOR);
        supportedChannels.add(PostgresChannelManager.HDFS_TSV_DESCRIPTOR);
        return supportedChannels;
    }

}