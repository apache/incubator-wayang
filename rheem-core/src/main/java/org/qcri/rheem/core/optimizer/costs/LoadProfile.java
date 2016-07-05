package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.
 */
public class LoadProfile {

    private final LoadEstimate cpuUsage, ramUsage, networkUsage, diskUsage;

    private final Collection<LoadProfile> subprofiles = new LinkedList<>();

    /**
     * The resource utilization of this load profile.
     */
    private double resourceUtilization = 1d;

    /**
     * Overhead time that occurs when working on this load profile.
     */
    private long overheadMillis;

    /**
     * Creates a new instance without network and disk usage, full resource utilization, and no overhead.
     *
     * @param cpuUsage the CPU load
     * @param ramUsage the RAM load
     */
    public LoadProfile(LoadEstimate cpuUsage, LoadEstimate ramUsage) {
        this(cpuUsage, ramUsage, null, null);
    }

    /**
     * Creates a new instance with full resource utilization, and no overhead.
     *
     * @param cpuUsage     the CPU load
     * @param ramUsage     the RAM load
     * @param networkUsage the network load
     * @param diskUsage    the disk load
     */
    public LoadProfile(LoadEstimate cpuUsage,
                       LoadEstimate ramUsage,
                       LoadEstimate networkUsage,
                       LoadEstimate diskUsage) {
        this(cpuUsage, ramUsage, networkUsage, diskUsage, 1d, 0);
    }

    /**
     * Creates a new instance.
     *
     * @param cpuUsage            the CPU load
     * @param ramUsage            the RAM load
     * @param networkUsage        the network load
     * @param diskUsage           the disk load
     * @param resourceUtilization resource utilization in the interval {@code [0, 1]}
     * @param overheadMillis      static overhead in milliseconds
     */
    public LoadProfile(LoadEstimate cpuUsage,
                       LoadEstimate ramUsage,
                       LoadEstimate networkUsage,
                       LoadEstimate diskUsage,
                       double resourceUtilization,
                       long overheadMillis) {
        this.cpuUsage = cpuUsage;
        this.ramUsage = ramUsage;
        this.networkUsage = networkUsage;
        this.diskUsage = diskUsage;
        this.overheadMillis = overheadMillis;
        this.resourceUtilization = resourceUtilization;
    }


    public LoadEstimate getCpuUsage() {
        return this.cpuUsage;
    }

    public LoadEstimate getRamUsage() {
        return this.ramUsage;
    }

    public LoadEstimate getNetworkUsage() {
        return this.networkUsage;
    }

    public LoadEstimate getDiskUsage() {
        return this.diskUsage;
    }

    public Collection<LoadProfile> getSubprofiles() {
        return this.subprofiles;
    }

    public void nest(LoadProfile subprofile) {
        this.subprofiles.add(subprofile);
    }

    public long getOverheadMillis() {
        return this.overheadMillis;
    }

    public void setOverheadMillis(long overheadMillis) {
        this.overheadMillis = overheadMillis;
    }

    public double getResourceUtilization() {
        return resourceUtilization;
    }

    public void setResourceUtilization(double resourceUtilization) {
        this.resourceUtilization = resourceUtilization;
    }

    /**
     * Multiplies the values of this instance and nested instances except for the RAM usage, which will not be altered.
     * This is to represent this instance occurring sequentially (not simultaneously) {@code n} times.
     *
     * @param n the factor to multiply with
     * @return the product
     */
    public LoadProfile timesSequential(int n) {
        if (n == 1) return this;

        LoadProfile product = new LoadProfile(
                this.cpuUsage.times(n),
                this.ramUsage,
                this.networkUsage != null ? this.networkUsage.times(n) : null,
                this.diskUsage != null ? this.diskUsage.times(n) : null,
                this.resourceUtilization,
                this.overheadMillis
        );
        for (LoadProfile subprofile : this.getSubprofiles()) {
            product.nest(subprofile.timesSequential(n));
        }
        return product;
    }
}
