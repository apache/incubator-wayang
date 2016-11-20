package org.qcri.rheem.core.optimizer.costs;

import org.json.JSONObject;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.util.JsonSerializable;
import org.qcri.rheem.core.util.JsonSerializables;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.
 */
public class LoadProfile implements JsonSerializable {

    /**
     * Instance with all values set to {@code 0}.
     */
    public static final LoadProfile emptyLoadProfile = new LoadProfile(
            new LoadEstimate(0),
            new LoadEstimate(0),
            new LoadEstimate(0),
            new LoadEstimate(0)
    );

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

    /**
     * Adds a this and the given instance.
     *
     * @param that the other summand
     * @return a new instance representing the sum
     */
    public LoadProfile plus(LoadProfile that) {
        return new LoadProfile(
                LoadEstimate.add(this.cpuUsage, that.cpuUsage),
                LoadEstimate.add(this.ramUsage, that.ramUsage),
                LoadEstimate.add(this.networkUsage, that.networkUsage),
                LoadEstimate.add(this.diskUsage, that.diskUsage),
                (this.resourceUtilization + that.resourceUtilization) / 2,
                this.overheadMillis + that.overheadMillis
        );
    }

    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("cpu", JsonSerializables.serialize(this.cpuUsage, false));
        json.put("ram", JsonSerializables.serialize(this.ramUsage, false));
        json.putOpt("network", JsonSerializables.serialize(this.networkUsage, false));
        json.putOpt("disk", JsonSerializables.serialize(this.diskUsage, false));
        json.put("utilization", this.resourceUtilization);
        json.put("overhead", this.overheadMillis);
        return json;
    }

    @SuppressWarnings("unused")
    public static LoadProfile fromJson(JSONObject jsonObject) {
        return new LoadProfile(
                JsonSerializables.deserialize(jsonObject.getJSONObject("cpu"), LoadEstimate.class),
                JsonSerializables.deserialize(jsonObject.getJSONObject("ram"), LoadEstimate.class),
                JsonSerializables.deserialize(jsonObject.optJSONObject("network"), LoadEstimate.class),
                JsonSerializables.deserialize(jsonObject.optJSONObject("disk"), LoadEstimate.class),
                jsonObject.getDouble("utilization"),
                jsonObject.getLong("overhead")
        );
    }
}
