package org.qcri.rheem.graphchi.operators;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;

/**
 * {@link ExecutionOperator} that can be run by the {@link GraphChiExecutor}.
 */
public interface GraphChiExecutionOperator extends ExecutionOperator {

    void execute(ChannelInstance[] inputChannels, ChannelInstance[] outputChannels, Configuration configuration);

}
