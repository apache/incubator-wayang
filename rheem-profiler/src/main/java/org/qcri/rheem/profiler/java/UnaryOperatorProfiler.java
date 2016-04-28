package org.qcri.rheem.profiler.java;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
 */
public class UnaryOperatorProfiler extends OperatorProfiler {

    private JavaChannelInstance inputChannelExecutor, outputChannelExecutor;

    public UnaryOperatorProfiler(Supplier<JavaExecutionOperator> operatorGenerator, Supplier<?> dataQuantumGenerator) {
        super(operatorGenerator, dataQuantumGenerator);
    }

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

        // Allocate output.
        this.outputChannelExecutor = createChannelExecutor();
    }


    public long executeOperator() {
        this.operator.evaluate(
                new JavaChannelInstance[]{this.inputChannelExecutor},
                new JavaChannelInstance[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        return this.outputChannelExecutor.provideStream().count();
    }

    @Override
    public JavaExecutionOperator getOperator() {
        return this.operator;
    }
}
