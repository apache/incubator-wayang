package org.qcri.rheem.core.optimizer.costs;

import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Calculates a {@link TimeEstimate} for a link {@link ResourceUsageProfile}.
 */
public abstract class ResourceUsageProfileToTimeConverter {

    protected final ResourceUsageToTimeConverter cpuConverter, diskCoverter, networkConverter;

    protected ResourceUsageProfileToTimeConverter(ResourceUsageToTimeConverter cpuConverter, ResourceUsageToTimeConverter diskCoverter, ResourceUsageToTimeConverter networkConverter) {
        this.cpuConverter = cpuConverter;
        this.diskCoverter = diskCoverter;
        this.networkConverter = networkConverter;
    }

    /**
     * Estimate the time required to execute something with the given {@code resourceUsageProfile}.
     */
    public abstract TimeEstimate convert(ResourceUsageProfile resourceUsageProfile);

    /**
     * Create an instance that adds up {@link TimeEstimate}s of given {@link ResourceUsageProfile}s including their
     * sub-{@link ResourceUsageProfile}s by adding up {@link TimeEstimate}s of the same type and otherwise using
     * the given objects to do the conversion.
     */
    public static ResourceUsageProfileToTimeConverter createDefault(ResourceUsageToTimeConverter cpuConverter,
                                                                    ResourceUsageToTimeConverter diskCoverter,
                                                                    ResourceUsageToTimeConverter networkConverter,
                                                                    ResourceTimeEstimateAggregator aggregator) {
        return new ResourceUsageProfileToTimeConverter(cpuConverter, diskCoverter, networkConverter) {

            @Override
            public TimeEstimate convert(ResourceUsageProfile resourceUsageProfile) {
                TimeEstimate cpuTime = this.sumWithSubprofiles(
                        resourceUsageProfile, ResourceUsageProfile::getCpuUsage, this.cpuConverter);
                TimeEstimate diskTime = this.sumWithSubprofiles(
                        resourceUsageProfile, ResourceUsageProfile::getDiskUsage, this.diskCoverter);
                TimeEstimate networkTime = this.sumWithSubprofiles(
                        resourceUsageProfile, ResourceUsageProfile::getNetworkUsage, this.networkConverter);

                return aggregator.aggregate(cpuTime, diskTime, networkTime);
            }

            private TimeEstimate sumWithSubprofiles(ResourceUsageProfile profile,
                                                    Function<ResourceUsageProfile, ResourceUsageEstimate> property,
                                                    ResourceUsageToTimeConverter converter) {
                return Stream.concat(Stream.of(profile), profile.getSubprofiles().stream())
                        .map(property::apply)
                        .filter(Objects::nonNull)
                        .map(converter::convert)
                        .reduce(TimeEstimate::plus)
                        .get();
            }

        };


    }

    @FunctionalInterface
    public interface ResourceTimeEstimateAggregator {

        TimeEstimate aggregate(TimeEstimate cpuEstimate, TimeEstimate diskEstimate, TimeEstimate networkEstimate);

    }

}
