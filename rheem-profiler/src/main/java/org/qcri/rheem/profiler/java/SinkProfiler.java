package org.qcri.rheem.profiler.java;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} implementation for sinks.
 */
public class SinkProfiler extends OperatorProfiler {

    private ChannelExecutor inputChannelExecutor;

    public SinkProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?>... dataQuantumGenerators) {
        super(operatorGenerator, dataQuantumGenerators);
    }

    @Override
    public void prepare(long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);

        super.prepare(inputCardinalities);
        int inputCardinality = (int) inputCardinalities[0];

        // Create input data.
        Collection<Object> dataQuanta = new ArrayList<>(inputCardinality);
        final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
        for (int i = 0; i < inputCardinality; i++) {
            dataQuanta.add(supplier.get());
        }
        this.inputChannelExecutor = createChannelExecutor(dataQuanta);
    }

    @Override
    protected long executeOperator() {
        this.operator.evaluate(
                new ChannelExecutor[]{this.inputChannelExecutor},
                new ChannelExecutor[]{},
                new FunctionCompiler()
        );
        return 0L;
    }

}
