package org.qcri.rheem.basic.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.core.util.Actions;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;

/**
 * Represents a {@link Channel} that is realized via a file/set of files.
 */
public class FileChannel extends Channel {

    public static final FileChannel.Descriptor HDFS_TSV_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "tsv");

    public static final FileChannel.Descriptor HDFS_OBJECT_FILE_DESCRIPTOR = new FileChannel.Descriptor("hdfs", "object-file");

    public FileChannel(FileChannel.Descriptor descriptor) {
        this(descriptor, null);
    }

    public FileChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
    }

    private FileChannel(FileChannel parent) {
        super(parent);
    }


    @Override
    public FileChannel copy() {
        return new FileChannel(this);
    }

    @Override
    public String toString() {
        return String.format("%s[%s->%s,%s,%s]",
                this.getClass().getSimpleName(),
                this.getProducer() == null ? this.getProducerSlot() : this.getProducer(),
                this.getConsumers(),
                this.getDescriptor().getLocation(),
                this.getDescriptor().getSerialization()
        );
    }
    @Override
    public FileChannel.Descriptor getDescriptor() {
        return (FileChannel.Descriptor) super.getDescriptor();
    }

    @Override
    public ChannelInstance createInstance(Executor executor, OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
        // NB: File channels are not inherent to a certain Platform, therefore are not tied to the executor.
        return new Instance(producerOperatorContext, producerOutputIndex);
    }

    /**
     * {@link ChannelDescriptor} for {@link FileChannel}s.
     */
    public static class Descriptor extends ChannelDescriptor {

        private final String location;

        private final String serialization;

        /**
         * Creates a new instance.
         *
         * @param location      file system of the file; use URL protocols here, e.g., {@code file}, {@code hdfs}, or
         *                      {@code tachyon}
         * @param serialization type of serialization, e.g., {@code object-file}, {@code tsv}
         */
        public Descriptor(String location, String serialization) {
            super(FileChannel.class, true, true);
            this.location = location;
            this.serialization = serialization;
        }

        public String getLocation() {
            return this.location;
        }

        public String getSerialization() {
            return this.serialization;
        }

        @Override
        public String toString() {
            return "Descriptor[" +
                    this.location + '\'' +
                    ", " + this.serialization + '\'' +
                    ']';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || this.getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Descriptor that = (Descriptor) o;
            return Objects.equals(this.location, that.location) &&
                    Objects.equals(this.serialization, that.serialization);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), this.location, this.serialization);
        }
    }

    /**
     * {@link ChannelInstance} implementation for {@link FileChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private Collection<String> paths = new LinkedList<>();

        /**
         * Creates a new instance.
         * @param producerOperatorContext
         * @param producerOutputIndex
         */
        protected Instance(OptimizationContext.OperatorContext producerOperatorContext, int producerOutputIndex) {
            super(null, producerOperatorContext, producerOutputIndex);
        }

        public FileChannel getChannel() {
            return FileChannel.this;
        }

        public void addPath(String path) {
            this.paths.add(path);
        }

        String generateTempPath(Configuration configuration) {
            final String tempDir = configuration.getStringProperty("rheem.basic.tempdir");
            Random random = new Random();
            return String.format("%s/%04x-%04x-%04x-%04x.tmp", tempDir,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF,
                    random.nextInt() & 0xFFFF
            );
        }

        public String addGivenOrTempPath(String pathOrNull, Configuration configuration) {
            final String path = pathOrNull == null ? this.generateTempPath(configuration) : pathOrNull;
            this.addPath(path);
            return path;
        }

        public Collection<String> getPaths() {
            return this.paths;
        }

        /**
         * If there is only a single element on {@link #getPaths()}, retrieves it. Otherwise, fails.
         *
         * @return the single element from {@link #getPaths()}
         */
        public String getSinglePath() {
            assert this.paths.size() == 1 : String.format("Unsupported number of paths in %s.", this.paths);
            return this.paths.iterator().next();
        }

        @Override
        public void doDispose() throws RheemException {
            Actions.doSafe(() -> {
                logger.info("Deleting file channel instances {}.", this.paths);
                final String path = this.getSinglePath();
                final Optional<FileSystem> fileSystemOptional = FileSystems.getFileSystem(path);
                fileSystemOptional.ifPresent(fs -> {
                    try {
                        fs.delete(path, true);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            });
        }
    }
}
