package org.qcri.rheem.graphchi.operators;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;

/**
 * {@link ExecutionOperator} that can be run by the {@link GraphChiExecutor}.
 */
public interface GraphChiOperator extends ExecutionOperator {

    void execute(Channel[] inputChannels, Channel[] outputChannels);

}
