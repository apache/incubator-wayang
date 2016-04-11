package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.spark.operators.SparkTextFileSource;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} for the {@link SparkTextFileSource}.
 */
public class SparkTextFileSourceProfiler extends SparkSourceProfiler {

    private final String fileUrl;

    public SparkTextFileSourceProfiler(Configuration configuration,
                                       Supplier<?> dataQuantumGenerator) {
        this(configuration.getStringProperty("rheem.profiler.datagen.url"), configuration, dataQuantumGenerator);
    }

    private SparkTextFileSourceProfiler(String fileUrl,
                                        Configuration configuration,
                                        Supplier<?> dataQuantumGenerator) {
        super(() -> new SparkTextFileSource(fileUrl), configuration, dataQuantumGenerator);
        this.fileUrl = fileUrl;
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;

        // Obtain access to the file system.
        final FileSystem fileSystem = FileSystems.getFileSystem(this.fileUrl).orElseThrow(
                () -> new RheemException(String.format("File system of %s not supported.", this.fileUrl))
        );

        // Try to delete any existing file.
        try {
            if (!fileSystem.delete(this.fileUrl, true)) {
                this.logger.warn("Could not delete {}.", this.fileUrl);
            }
        } catch (IOException e) {
            this.logger.error(String.format("Deleting %s failed.", this.fileUrl), e);
        }

        // Generate and write the test data.
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        fileSystem.create(this.fileUrl),
                        "UTF-8"
                )
        )) {
            final Supplier<?> supplier = this.dataQuantumGenerators.get(0);
            for (long i = 0; i < inputCardinality; i++) {
                writer.write(supplier.get().toString());
                writer.write('\n');
            }
        } catch (Exception e) {
            throw new RheemException(String.format("Could not write test data to %s.", this.fileUrl), e);
        }
    }

    @Override
    public void cleanUp() {
        super.cleanUp();

        FileSystems.getFileSystem(this.fileUrl).ifPresent(fs -> {
                    try {
                        fs.delete(this.fileUrl, true);
                    } catch (IOException e) {
                        this.logger.error(String.format("Could not delete profiling file %s.", this.fileUrl), e);
                    }
                }
        );
    }
}
