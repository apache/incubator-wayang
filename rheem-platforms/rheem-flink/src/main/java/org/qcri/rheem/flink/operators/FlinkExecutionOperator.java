package org.qcri.rheem.flink.operators;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.execution.FlinkExecutor;
import org.qcri.rheem.flink.platform.FlinkPlatform;

import java.io.Serializable;
import java.util.Collection;

/**
 * Execution operator for the Flink platform.
 */
public interface FlinkExecutionOperator extends ExecutionOperator, Serializable {

    @Override
    default FlinkPlatform getPlatform() {
        return FlinkPlatform.getInstance();
    }

    Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) throws Exception;

    /**
     * Tell whether this instances is a Flink action. This is important to keep track on when Flink is actually
     * initialized.
     *
     * @return whether this instance issues Flink actions
     */
    boolean containsAction();

    default <Type> Collection<Type> getBroadCastFunction(String name){
        return null;
    }

}
