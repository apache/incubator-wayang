package org.qcri.rheem.core.optimizer.costs;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.ExecutionOperator;
import org.qcri.rheem.core.plan.InputSlot;
import org.qcri.rheem.core.plan.Operator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Reflects the (estimated) required resources of an {@link Operator} or {@link FunctionDescriptor}.
 */
public class LoadProfile {

    private final LoadEstimate cpuUsage, ramUsage, networkUsage, diskUsage;

    private final Collection<LoadProfile> subprofiles = new LinkedList<>();

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
        return cpuUsage;
    }

    public LoadEstimate getRamUsage() {
        return ramUsage;
    }

    public LoadEstimate getNetworkUsage() {
        return networkUsage;
    }

    public LoadEstimate getDiskUsage() {
        return diskUsage;
    }

    public Collection<LoadProfile> getSubprofiles() {
        return subprofiles;
    }

    public void nest(LoadProfile subprofile) {
        this.subprofiles.add(subprofile);
    }
}
