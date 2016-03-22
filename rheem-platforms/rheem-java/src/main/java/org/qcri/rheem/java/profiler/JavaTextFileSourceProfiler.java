package org.qcri.rheem.java.profiler;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.java.channels.ChannelExecutor;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for sources.
 */
public class JavaTextFileSourceProfiler extends OperatorProfiler {

    private ChannelExecutor outputChannelExecutor;

    private File tempFile;

    public JavaTextFileSourceProfiler(Supplier<String> dataQuantumGenerator) {
        super(null, dataQuantumGenerator);
        this.operatorGenerator = this::createOperator;
    }

    private JavaTextFileSource createOperator() {
        return new JavaTextFileSource(this.tempFile.toURI().toString());
    }

    @Override
    public void prepare(long... inputCardinalities) {
        Validate.isTrue(inputCardinalities.length == 1);
        try {
            if (this.tempFile != null) {
                this.tempFile.delete();
            }
            this.tempFile = File.createTempFile("rheem-java", "txt");
            this.tempFile.deleteOnExit();

            super.prepare(inputCardinalities);
            int inputCardinality = (int) inputCardinalities[0];

            // Create input data.
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.tempFile))) {
                final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
                for (int i = 0; i < inputCardinality; i++) {
                    writer.write(supplier.get().toString());
                    writer.write('\n');
                }
            }

            // Hacky.
            this.operator = new JavaTextFileSource(this.tempFile.toURI().toString());

            // Allocate output.
            this.outputChannelExecutor = createChannelExecutor();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected long provideDiskBytes() {
        return this.tempFile.length();
    }

    @Override
    public long executeOperator() {
        this.operator.evaluate(
                new ChannelExecutor[]{},
                new ChannelExecutor[]{this.outputChannelExecutor},
                new FunctionCompiler()
        );
        return this.outputChannelExecutor.provideStream().count();
    }

}
