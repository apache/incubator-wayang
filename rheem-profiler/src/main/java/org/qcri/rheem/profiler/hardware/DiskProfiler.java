package org.qcri.rheem.profiler.hardware;

import org.apache.commons.lang.Validate;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.qcri.rheem.profiler.util.ProfilingUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Profiles the reading and writing speed to some directory.
 */
public class DiskProfiler {

    private final String testFileURl;

    private final FileSystem fs;

    public DiskProfiler(String testFileURl) {
        this.testFileURl = testFileURl;
        this.fs = FileSystems.getFileSystem(this.testFileURl).orElse(null);
        Validate.notNull(this.fs);
    }

    /**
     * Writes and reads a file and measures the elapsed time.
     *
     * @param sizeInMB the number of MB to write/read
     * @return a CSV line like {@code MB,write nanos,read nanos}
     */
    public String profile(int sizeInMB) {
        long writeNanos = this.profileWriting(sizeInMB);
        System.out.println("Sleeping for 10 sec...");
        ProfilingUtils.sleep(10000);
        long readNanos = this.profileReading(sizeInMB);

        return String.format("%d,%d,%d", sizeInMB, writeNanos, readNanos);
    }

    /**
     * Writes a file and measures the time required to do so.
     *
     * @return the number of nanos used to write
     */
    private long profileWriting(int sizeInMB) {
        // Generate some random data (don't use empty data, as, e.g., NTFS does not write it to disk).
        byte[] mb = new byte[1024 * 1024];
        new Random().nextBytes(mb);

        // Write the requested amount of MB and measure.
        System.out.printf("Start writing %d MB... ", sizeInMB);
        long startWriteTime = System.nanoTime();
        int writtenInMB = 0;
        try (final OutputStream outputStream = this.fs.create(this.testFileURl)) {
            while (writtenInMB < sizeInMB) {
                outputStream.write(mb);
                writtenInMB++;
            }
        } catch (IOException e) {
            System.err.println("Profile writing failed.");
            e.printStackTrace();
        }
        long endWriteTime = System.nanoTime();
        System.out.printf("done.");

        System.out.printf("Writing %d MB to %s completed in %s.\n",
                sizeInMB, this.testFileURl, Formats.formatDuration((endWriteTime - startWriteTime) / 1000 / 1000, true)
        );

        return endWriteTime - startWriteTime;
    }

    /**
     * Reads a file and measures the time required to do so.
     *
     * @return the number of nanos used to read
     */
    private long profileReading(int sizeInMB) {
        byte[] mb = new byte[1024 * 1024];

        // Write the requested amount of MB and measure.
        System.out.printf("Start reading %d MB... ", sizeInMB);
        long startReadTime = System.nanoTime();
        long readBytes = 0; // for sanity checking
        try (final InputStream inputStream = this.fs.open(this.testFileURl)) {
            long b;
            while ((b = inputStream.read(mb)) != -1) readBytes += b;
        } catch (IOException e) {
            System.err.println("Profile reading failed.");
            e.printStackTrace();
        }
        long endReadTime = System.nanoTime();
        System.out.println("done.");
        long expectedBytes = sizeInMB * 1024L * 1024L;
        if (expectedBytes != readBytes) {
            System.out.printf("Warning: Expected %d bytes, but found %d.\n", expectedBytes, readBytes);
            sizeInMB = (int) readBytes / 1024 / 1024;
        }

        System.out.printf("Reading %d MB from %s completed in %s.\n",
                sizeInMB, this.testFileURl, Formats.formatDuration((endReadTime - startReadTime) / 1000 / 1000, true)
        );

        return endReadTime - startReadTime;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.printf(
                    "Usage: java ... %s <URL to profile> <MB to use>[,<MB to use>]*\n",
                    DiskProfiler.class
            );
            System.exit(1);
        }

        List<String> measurementCsvRows = new LinkedList<>();
        DiskProfiler diskProfiler = new DiskProfiler(args[0]);
        for (String arg : args[1].split(",")) {
            int sizeInMB = Integer.parseInt(arg);
            ProfilingUtils.sleep(1000);
            final String measurementCsvRow = diskProfiler.profile(sizeInMB);
            measurementCsvRows.add(measurementCsvRow);
        }

        System.out.println();
        System.out.println("size_in_mb,write_nanos,read_nanos");
        measurementCsvRows.forEach(System.out::println);
    }
}
