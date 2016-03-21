package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} specifically for {@link JavaExecutionOperator}s with a single {@link InputSlot}.
 */
public class UnaryOperatorProfiler<In> extends OperatorProfiler {

    private final Supplier<In> dataQuantumGenerator;

    private final Supplier<JavaExecutionOperator> operatorGenerator;

    private JavaExecutionOperator operator;

    private ChannelExecutor inputChannelExecutor, outputChannelExecutor;

    public UnaryOperatorProfiler(Supplier<In> dataQuantumGenerator, Supplier<JavaExecutionOperator> operatorGenerator) {
        this.dataQuantumGenerator = dataQuantumGenerator;
        this.operatorGenerator = operatorGenerator;
    }

    public void prepare(int inputCardinality) {
        // Create operator.
        this.operator = this.operatorGenerator.get();

        // Create input data.
        Collection<In> dataQuanta = new ArrayList<>(inputCardinality);
        for (int i = 0; i < inputCardinality; i++) {
            dataQuanta.add(this.dataQuantumGenerator.get());
        }
        this.inputChannelExecutor = createChannelExecutor();
        this.inputChannelExecutor.acceptCollection(dataQuanta);

        // Allocate output.
        this.outputChannelExecutor = createChannelExecutor();
    }



    public void run() {
        this.operator.evaluate(
                new ChannelExecutor[]{this.inputChannelExecutor},
                new ChannelExecutor[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        this.outputChannelExecutor.provideStream().forEach(x -> {});
    }

    @Override
    public JavaExecutionOperator getOperator() {
        return this.operator;
    }
}
