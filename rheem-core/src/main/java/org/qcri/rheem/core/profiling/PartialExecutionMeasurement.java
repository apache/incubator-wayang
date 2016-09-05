package org.qcri.rheem.core.profiling;

import de.hpi.isg.profiledb.store.model.Measurement;
import de.hpi.isg.profiledb.store.model.Type;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.PartialExecution;

import java.util.LinkedList;
import java.util.List;

/**
 * This {@link Measurement} adapts a {@link PartialExecutionMeasurement}.
 */
@Type("partial-execution")
public class PartialExecutionMeasurement extends Measurement {

    /**
     * @see PartialExecution#getOperatorContexts()
     */
    private List<Operator> operators = new LinkedList<>();

    /**
     * @see PartialExecution#getMeasuredExecutionTime()
     */
    private long executionMillis;

    /**
     * @see PartialExecution#getOverallTimeEstimate()
     */
    private TimeEstimate estimatedExecutionMillis;


    /**
     * Serialization constructor.
     */
    private PartialExecutionMeasurement() {
    }

    /**
     * Creates a new instance.
     *
     * @param id               the ID of the new instance
     * @param partialExecution provides data for the new instance
     */
    public PartialExecutionMeasurement(String id, PartialExecution partialExecution) {
        super(id);
        partialExecution.getOperatorContexts().stream()
                .map(OptimizationContext.OperatorContext::getOperator)
                .forEach(this.operators::add);
        this.executionMillis = partialExecution.getMeasuredExecutionTime();
        this.estimatedExecutionMillis = partialExecution.getOverallTimeEstimate();
    }

}
