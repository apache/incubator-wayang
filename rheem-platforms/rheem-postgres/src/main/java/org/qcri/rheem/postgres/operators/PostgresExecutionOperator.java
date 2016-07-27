package org.qcri.rheem.postgres.operators;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.postgres.PostgresPlatform;

import java.util.Collections;
import java.util.List;

public interface PostgresExecutionOperator extends ExecutionOperator {

    @Override
    default Platform getPlatform() {
        return PostgresPlatform.getInstance();
    }

    @Override
    default List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(PostgresPlatform.getInstance().getSqlQueryChannelDescriptor());
    }

    @Override
    default List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(PostgresPlatform.getInstance().getSqlQueryChannelDescriptor());
    }

}