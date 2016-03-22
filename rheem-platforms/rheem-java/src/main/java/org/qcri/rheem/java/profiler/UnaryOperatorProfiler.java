package org.qcri.rheem.java.profiler;

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
public class UnaryOperatorProfiler<In> extends OperatorProfiler {

    private final Supplier<In> dataQuantumGenerator;

    private final Supplier<JavaExecutionOperator> operatorGenerator;

    private JavaExecutionOperator operator;

    private ChannelExecutor inputChannelExecutor, outputChannelExecutor;

    public UnaryOperatorProfiler(Supplier<In> dataQuantumGenerator, Supplier<JavaExecutionOperator> operatorGenerator) {
        this.dataQuantumGenerator = dataQuantumGenerator;
        this.operatorGenerator = operatorGenerator;
    }

    public void prepare(long... inputCardinalities) {
        super.prepare(inputCardinalities);
        int inputCardinality = (int) inputCardinalities[0];

        // Create operator.
        this.operator = this.operatorGenerator.get();
        assert inputCardinalities.length == operator.getNumInputs();

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



    public long executeOperator() {
        this.operator.evaluate(
                new ChannelExecutor[]{this.inputChannelExecutor},
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
