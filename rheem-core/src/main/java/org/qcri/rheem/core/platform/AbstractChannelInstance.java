package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.lineage.ChannelLineageNode;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Template for {@link ChannelInstance} implementations.
 */
public abstract class AbstractChannelInstance extends ExecutionResourceTemplate implements ChannelInstance {

    private OptionalLong measuredCardinality = OptionalLong.empty();

    private boolean wasProduced = false;

    /**
     * The {@link OptimizationContext.OperatorContext} of the {@link ExecutionOperator} that is producing this
     * instance.
     */
    private final OptimizationContext.OperatorContext producerOperatorContext;

    private ChannelLineageNode lineage;

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
        this.lineage = new ChannelLineageNode(this);
        this.producerOperatorContext = producerOperatorContext;
    }

    @Override
    public OptionalLong getMeasuredCardinality() {
        return this.measuredCardinality;
    }

    @Override
    public void setMeasuredCardinality(long cardinality) {
        this.measuredCardinality.ifPresent(oldCardinality -> {
            if (oldCardinality != cardinality) {
                LoggerFactory.getLogger(this.getClass()).warn(
                        "Replacing existing measured cardinality of {} with {} (was {}).",
                        this.getChannel(),
                        cardinality,
                        oldCardinality
                );
            }
        });
        this.measuredCardinality = OptionalLong.of(cardinality);
    }

    @Override
    public ChannelLineageNode getLineage() {
        return this.lineage;
    }

    @Override
    public boolean wasProduced() {
        return this.wasProduced;
    }

    @Override
    public void markProduced() {
        this.wasProduced = true;
    }

    @Override
    public OptimizationContext.OperatorContext getProducerOperatorContext() {
        return this.producerOperatorContext;
    }

    @Override
    public String toString() {
        return "*" + this.getChannel().toString();
    }
}
