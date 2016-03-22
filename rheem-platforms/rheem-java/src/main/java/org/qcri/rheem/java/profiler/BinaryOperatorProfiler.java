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
public class BinaryOperatorProfiler<In0, In1> extends OperatorProfiler {

    private final Supplier<In0> dataQuantumGenerator0;

    private final Supplier<In1> dataQuantumGenerator1;

    private final Supplier<JavaExecutionOperator> operatorGenerator;

    private JavaExecutionOperator operator;

    private ChannelExecutor inputChannelExecutor0, inputChannelExecutor1, outputChannelExecutor;

    public BinaryOperatorProfiler(Supplier<In0> dataQuantumGenerator0,
                                  Supplier<In1> dataQuantumGenerator1,
                                  Supplier<JavaExecutionOperator> operatorGenerator) {
        this.dataQuantumGenerator0 = dataQuantumGenerator0;
        this.dataQuantumGenerator1 = dataQuantumGenerator1;
        this.operatorGenerator = operatorGenerator;
    }

    public void prepare(long... inputCardinalities) {
        super.prepare(inputCardinalities);

        // Create operator.
        this.operator = this.operatorGenerator.get();
        assert  inputCardinalities.length == this.operator.getNumInputs();
        int inputCardinality0 = (int) inputCardinalities[0];
        int inputCardinality1 = (int) inputCardinalities[1];

        // Create input data.
        Collection<In0> dataQuanta0 = new ArrayList<>(inputCardinality0);
        for (int i = 0; i < inputCardinality0; i++) {
            dataQuanta0.add(this.dataQuantumGenerator0.get());
        }
        this.inputChannelExecutor0 = createChannelExecutor(dataQuanta0);

        Collection<In1> dataQuanta1 = new ArrayList<>(inputCardinality1);
        for (int i = 0; i < inputCardinality1; i++) {
            dataQuanta1.add(this.dataQuantumGenerator1.get());
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
