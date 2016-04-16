package org.qcri.rheem.profiler.java;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} implementation for sinks.
 */
public abstract class SourceProfiler extends OperatorProfiler {

    private ChannelExecutor outputChannelExecutor;

    public SourceProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?>... dataQuantumGenerators) {
        super(operatorGenerator, dataQuantumGenerators);
    }

    @Override
    public void prepare(long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);

        try {
            this.setUpSourceData(inputCardinalities[0]);
        } catch (Exception e) {
            LoggerFactory.getLogger(this.getClass()).error(
                    String.format("Failed to set up source data for input cardinality %d.", inputCardinalities[0]),
                    e
            );
        }

        super.prepare(inputCardinalities);

        this.outputChannelExecutor = createChannelExecutor();
    }

    abstract void setUpSourceData(long cardinality) throws Exception;

    @Override
    protected long executeOperator() {
        this.operator.evaluate(
                new ChannelExecutor[]{},
                new ChannelExecutor[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        return this.outputChannelExecutor.provideStream().count();
    }

}
