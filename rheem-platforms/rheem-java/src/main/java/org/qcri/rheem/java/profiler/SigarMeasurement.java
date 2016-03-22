package org.qcri.rheem.java.profiler;

import org.hyperic.sigar.*;

/**
 * Encapsulates data measured using {@link Sigar}.
 */
public class SigarMeasurement {

    private final long timestamp;

    private final long diskReadBytes;

    private final long diskWriteBytes;

    private SigarMeasurement(long timestamp, long diskReadBytes, long diskWriteBytes) {
        this.timestamp = timestamp;
        this.diskReadBytes = diskReadBytes;
        this.diskWriteBytes = diskWriteBytes;
    }

    public static SigarMeasurement measure(final Sigar sigar, final FileSystem[] fileSystems) throws SigarException {
        final long startTimestamp = System.currentTimeMillis();

        long readBytes = 0, writeBytes = 0;
        final FileSystem[] fileSystemList = sigar.getFileSystemList();
        for (FileSystem fileSystem : fileSystemList) {
            final FileSystemUsage diskUsage = sigar.getFileSystemUsage(fileSystem.getDirName());
            if (diskUsage.getDiskReadBytes() != Sigar.FIELD_NOTIMPL) {
                readBytes += diskUsage.getDiskReadBytes();
                writeBytes += diskUsage.getDiskWriteBytes();
            }
        }

        final long stopTimestamp = System.currentTimeMillis();

        return new SigarMeasurement(stopTimestamp + startTimestamp / 2, readBytes, writeBytes);
    }
}
