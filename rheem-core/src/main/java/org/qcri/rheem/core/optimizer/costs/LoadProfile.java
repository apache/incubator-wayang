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
     * If not {@code -1}, tells the maximum number of machines/cores utilized in this {@link LoadProfile}. Preferred
     * over {@link #ratioMachines} and {@link #ratioCores}, respectively.
     */
    private int maxMachines = -1, maxCores = -1;


    /**
     * If not {@link Double#NaN}, tells the ratio of available machines/cores utilized in this {@link LoadProfile}.
     * Only considered if {@link #maxMachines} and {@link #maxCores}, respectively, are not specified.
     */
    private double ratioMachines = Double.NaN, ratioCores = Double.NaN;

    /**
     * Overhead time that occurs when working on this load profile.
     */
    private long overheadMillis;

    public LoadProfile(LoadEstimate cpuUsage,
                        LoadEstimate ramUsage,
                        LoadEstimate networkUsage,
                        LoadEstimate diskUsage) {
        this.cpuUsage = cpuUsage;
        this.ramUsage = ramUsage;
        this.networkUsage = networkUsage;
        this.diskUsage = diskUsage;
    }

    public LoadProfile(LoadEstimate cpuUsage, LoadEstimate ramUsage) {
        this(cpuUsage, ramUsage, null, null);
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

    @Deprecated
    public void setMaxCores(int maxCores) {
        this.maxCores = maxCores;
    }

    @Deprecated
    public void setMaxMachines(int maxMachines) {
        this.maxMachines = maxMachines;
    }

    @Deprecated
    public void setRatioCores(double ratioCores) {
        this.ratioCores = ratioCores;
    }

    public void setRatioMachines(double ratioMachines) {
        this.ratioMachines = ratioMachines;
    }

    @Deprecated
    public int getNumUsedMachines(int numAvailableMachines) {
        if (this.maxMachines != -1) {
            return Math.min(this.maxMachines, numAvailableMachines);
        }

        if (!Double.isNaN(this.ratioMachines)) {
            return (int) Math.max(1, Math.round(this.ratioMachines * numAvailableMachines));
        }

        return numAvailableMachines;
    }

    @Deprecated
    public int getNumUsedCores(int numAvailableCores) {
        if (this.maxCores != -1) {
            return Math.min(this.maxCores, numAvailableCores);
        }

        if (!Double.isNaN(this.ratioCores)) {
            return (int) Math.max(1, Math.round(this.ratioCores * numAvailableCores));
        }

        return numAvailableCores;
    }

    public double getRatioCores() {
        return this.ratioCores;
    }

    public double getRatioMachines() {
        return ratioMachines;
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

    /**
     * Multiplies the values of this instance and nested instances except for the RAM usage, which will not be altered.
     * This is to represent this instance occurring sequentially (not simultaneously) {@code n} times.
     * @param n the factor to multiply with
     * @return the product
     */
    public LoadProfile timesSequential(int n) {
        if (n == 1) return this;

        LoadProfile product = new LoadProfile(
                this.cpuUsage.times(n),
                this.ramUsage,
                this.networkUsage != null ? this.networkUsage.times(n) : null,
                this.diskUsage != null ? this.diskUsage.times(n) : null
        );
        if (this.maxCores != -1) product.maxCores = this.maxCores;
        if (this.maxMachines != -1) product.maxMachines = this.maxMachines;
        if (!Double.isNaN(this.ratioCores)) product.ratioCores = this.ratioCores;
        if (!Double.isNaN(this.ratioMachines)) product.ratioMachines = this.ratioMachines;
        product.overheadMillis = n * this.overheadMillis;
        for (LoadProfile subprofile : this.getSubprofiles()) {
            product.nest(subprofile.timesSequential(n));
        }
        return product;
    }
}
