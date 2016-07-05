package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Template for {@link ChannelInstance} implementations.
 */
public abstract class AbstractChannelInstance extends ExecutionResourceTemplate implements ChannelInstance {

    private OptionalLong measuredCardinality = OptionalLong.empty();

    private boolean wasExecuted = false;

    private LazyChannelLineage lazyChannelLineage;

    /**
     * Creates a new instance and registers it with its {@link Executor}.
     *
     * @param executor                that maintains this instance
     * @param producerOperatorContext the {@link OptimizationContext.OperatorContext} for the producing
     *                                {@link ExecutionOperator}
     * @param producerOutputIndex     the output index of the producer {@link ExecutionTask}
     */
    protected AbstractChannelInstance(Executor executor,
                                      OptimizationContext.OperatorContext producerOperatorContext,
                                      int producerOutputIndex) {
        super(executor);
        this.lazyChannelLineage = new LazyChannelLineage(this, producerOperatorContext, producerOutputIndex);
    }

    @Override
    public OptionalLong getMeasuredCardinality() {
        return this.measuredCardinality;
    }

    @Override
    public void setMeasuredCardinality(long cardinality) {
        this.measuredCardinality.ifPresent(oldCardinality ->
                LoggerFactory.getLogger(this.getClass()).warn(
                        "Replacing existing measured cardinality of {} with {} (was {}).",
                        this.getChannel(),
                        cardinality,
                        oldCardinality
                )
        );
        this.measuredCardinality = OptionalLong.of(cardinality);
    }

    @Override
    public LazyChannelLineage getLazyChannelLineage() {
        return this.lazyChannelLineage;
    }

    @Override
    public boolean wasExecuted() {
        return this.wasExecuted;
    }

    @Override
    public void markExecuted() {
        this.wasExecuted = true;
    }

    @Override
    public String toString() {
        return "*" + this.getChannel().toString();
    }
}
