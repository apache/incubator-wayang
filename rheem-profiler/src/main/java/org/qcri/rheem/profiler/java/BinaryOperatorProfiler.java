package org.qcri.rheem.profiler.java;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
 */
public class BinaryOperatorProfiler extends OperatorProfiler {

    private ChannelExecutor inputChannelExecutor0, inputChannelExecutor1, outputChannelExecutor;

    public BinaryOperatorProfiler(Supplier<JavaExecutionOperator> operatorGenerator,
                                  Supplier<?> dataQuantumGenerator0,
                                  Supplier<?> dataQuantumGenerator1) {
        super(operatorGenerator, dataQuantumGenerator0, dataQuantumGenerator1);
    }

    public void prepare(long... inputCardinalities) {
        assert inputCardinalities.length == 2;

        super.prepare(inputCardinalities);

        // Create operator.
        assert inputCardinalities.length == this.operator.getNumInputs();
        int inputCardinality0 = (int) inputCardinalities[0];
        int inputCardinality1 = (int) inputCardinalities[1];

        // Create input data.
        Collection<Object> dataQuanta0 = new ArrayList<>(inputCardinality0);
        final Supplier<?> supplier0 = this.dataQuantumGenerators.get(0);
        for (int i = 0; i < inputCardinality0; i++) {
            dataQuanta0.add(supplier0.get());
        }
        this.inputChannelExecutor0 = createChannelExecutor(dataQuanta0);

        Collection<Object> dataQuanta1 = new ArrayList<>(inputCardinality1);
        final Supplier<?> supplier1 = this.dataQuantumGenerators.get(1);
        for (int i = 0; i < inputCardinality1; i++) {
            dataQuanta1.add(supplier1.get());
        }
        this.inputChannelExecutor1 = createChannelExecutor(dataQuanta1);

        // Allocate output.
        this.outputChannelExecutor = createChannelExecutor();
    }


    public long executeOperator() {
        this.operator.evaluate(
                new ChannelExecutor[]{this.inputChannelExecutor0, this.inputChannelExecutor1},
                new ChannelExecutor[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        return this.outputChannelExecutor.provideStream().count();
    }

    @Override
    public JavaExecutionOperator getOperator() {
        return this.operator;
    }
}
