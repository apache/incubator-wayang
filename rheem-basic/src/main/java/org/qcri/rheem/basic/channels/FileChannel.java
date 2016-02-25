package org.qcri.rheem.basic.channels;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.platform.ChannelDescriptor;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Objects;

/**
 * Represents a {@link Channel} that is realized via a file/set of files.
 */
public class FileChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = false;

    private Collection<String> paths = new LinkedList<>();

    public FileChannel(FileChannel.Descriptor descriptor,
                       ExecutionTask producer,
                       int outputIndex,
                       CardinalityEstimate cardinalityEstimate) {
        super(descriptor, producer, outputIndex, cardinalityEstimate);
    }

    private FileChannel(FileChannel parent) {
        super(parent);
        this.paths.addAll(parent.getPaths());
    }

    public void addPath(String path) {
        this.paths.add(path);
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
        assert this.paths.size() == 1;
        return this.paths.iterator().next();
    }

    @Override
    public boolean isReusable() {
        return IS_REUSABLE;
    }

    @Override
    public boolean isInterStageCapable() {
        return IS_REUSABLE;
    }

    @Override
    public boolean isInterPlatformCapable() {
        return IS_REUSABLE & !IS_INTERNAL;
    }

    @Override
    public FileChannel copy() {
        return new FileChannel(this);
    }

    @Override
    public String toString() {
        return String.format("%s%s", this.getClass().getSimpleName(), this.paths);
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
            super(FileChannel.class);
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
}
