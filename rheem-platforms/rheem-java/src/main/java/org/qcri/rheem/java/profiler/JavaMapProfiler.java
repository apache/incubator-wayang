package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

/**
 * TODO
 */
public class JavaMapProfiler<In, Out> {

    private final Supplier<In> dataQuantumGenerator;

    private final Supplier<JavaMapOperator<In, Out>> operatorGenerator;

    private JavaMapOperator<In, Out> operator;

    private ChannelExecutor inputChannelExecutor, outputChannelExecutor;

    public JavaMapProfiler(Supplier<In> dataQuantumGenerator, Supplier<JavaMapOperator<In, Out>> operatorGenerator) {
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
        final DataSetType<In> inputType = this.operator.getInput().getType();
        this.inputChannelExecutor = this.createChannelExecutor();
        this.inputChannelExecutor.acceptCollection(dataQuanta);

        // Allocate output.
        this.outputChannelExecutor = this.createChannelExecutor();
    }

    private ChannelExecutor createChannelExecutor() {
        final ChannelDescriptor channelDescriptor = StreamChannel.DESCRIPTOR;
        final StreamChannel streamChannel = new StreamChannel(channelDescriptor, null);
        return JavaPlatform.getInstance().getChannelManager().createChannelExecutor(streamChannel);
    }

    public void run() {
        this.operator.evaluate(
                new ChannelExecutor[]{this.inputChannelExecutor},
                new ChannelExecutor[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        this.outputChannelExecutor.provideStream().forEach(x -> {});
    }

}
