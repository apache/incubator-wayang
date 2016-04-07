package org.qcri.rheem.profiler.java;

import org.qcri.rheem.java.operators.JavaTextFileSource;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.function.Supplier;

/**
 * {@link OperatorProfiler} for sources.
 */
public class JavaTextFileSourceProfiler extends SourceProfiler {

    private File tempFile;

    public JavaTextFileSourceProfiler(Supplier<String> dataQuantumGenerator) {
        super(null, dataQuantumGenerator);
        this.operatorGenerator = this::createOperator; // We can only pass the method reference after super(...).
    }

    private JavaTextFileSource createOperator() {
        return new JavaTextFileSource(this.tempFile.toURI().toString());
    }

    @Override
    void setUpSourceData(long cardinality) throws Exception {
        if (this.tempFile != null) {
            if (!this.tempFile.delete()) {
                this.logger.warn("Could not delete {}.", this.tempFile);
            }
        }
        this.tempFile = File.createTempFile("rheem-java", "txt");
        this.tempFile.deleteOnExit();

        // Create input data.
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(this.tempFile))) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (int i = 0; i < cardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        }
    }

    @Override
    protected long provideDiskBytes() {
        return this.tempFile.length();
    }


}
