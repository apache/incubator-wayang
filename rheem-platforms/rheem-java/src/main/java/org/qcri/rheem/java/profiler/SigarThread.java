package org.qcri.rheem.java.profiler;

import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.util.LinkedList;
import java.util.List;

/**
 * {@link Thread} that runs {@link Sigar} in the background to obtain profiling data.
 */
public class SigarThread extends Thread {

    /**
     * Obtains measurements.
     */
    private final Sigar sigar;

    /**
     * {@link FileSystem}s to monitor for disk usage.
     */
    private final FileSystem[] fileSystems;

    /**
     * Whether this instance is suspended from its measuring activity.
     */
    private boolean isSuspended = true;

    /**
     * Whether this instance is condemned to stop.
     */
    private boolean isCondemned = false;

    /**
     * Desired measurement frequency.
     */
    private final long measurementFrequency;

    private final List<SigarMeasurement> measurements = new LinkedList<>();

    public SigarThread(long measurementFrequency) {
        this.measurementFrequency = measurementFrequency;
        this.sigar = new Sigar();
        try {
            this.fileSystems = this.sigar.getFileSystemList();
        } catch (SigarException e) {
            throw new RuntimeException("Could not list file systems for monitoring.", e);
        }

    }

    @Override
    public void run() {
        try {
            // Main loop.
            while (!this.isCondemned) {
                // Do work if requested.
                final long startTimestamp = System.currentTimeMillis();
                if (!this.isSuspended) {
                    this.measurements.add(SigarMeasurement.measure(this.sigar, this.fileSystems));
                }

                // Sleep requested amount of time.
                final long stopTimestamp = System.currentTimeMillis();
                final long measurementDuration = stopTimestamp - startTimestamp;
                final long sleepTime = this.measurementFrequency - measurementDuration;
                if (sleepTime > 0) {
                    sleep(sleepTime);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(2);
        }
    }

    public void measure() {
        assert this.isCondemned == false;
        this.isSuspended = false;
    }

    public void idle() {
        this.isSuspended = true;
        Thread.yield();
    }

    public void finish() {
        this.isSuspended = true;
        this.isCondemned = true;

        // Try to wait for the instance to finish.
        if (this != Thread.currentThread()) {
            try {
                this.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public List<SigarMeasurement> getMeasurements() {
        return this.measurements;
    }
}
