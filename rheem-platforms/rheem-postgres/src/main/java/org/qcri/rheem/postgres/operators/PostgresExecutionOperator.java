package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.PostgresPlatform;
import org.qcri.rheem.postgres.channels.PostgresChannelManager;

import java.util.Collections;
import java.util.List;

public interface PostgresExecutionOperator extends ExecutionOperator {
    void evaluate(Channel[] inputChannels, Channel[] outputChannels);


    @Override
    default Platform getPlatform() {
        return PostgresPlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new RheemException(String.format("Input channels for postgres not implemented"));
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(PostgresChannelManager.HDFS_TSV_DESCRIPTOR);
    }

}