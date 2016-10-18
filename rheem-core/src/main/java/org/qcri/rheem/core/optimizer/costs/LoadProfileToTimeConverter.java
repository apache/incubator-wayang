package org.qcri.rheem.core.optimizer.costs;

import java.util.Objects;
import java.util.function.Function;

/**
 * Calculates a {@link TimeEstimate} for a link {@link LoadProfile}.
 */
public abstract class LoadProfileToTimeConverter {

    protected final LoadToTimeConverter cpuConverter, diskCoverter, networkConverter;

    protected LoadProfileToTimeConverter(LoadToTimeConverter cpuConverter, LoadToTimeConverter diskCoverter, LoadToTimeConverter networkConverter) {
        this.cpuConverter = cpuConverter;
        this.diskCoverter = diskCoverter;
        this.networkConverter = networkConverter;
    }

    /**
     * Estimate the time required to execute something with the given {@code loadProfile}.
     */
    public abstract TimeEstimate convert(LoadProfile loadProfile);

    /**
     * Create an instance that adds up {@link TimeEstimate}s of given {@link LoadProfile}s including their
     * sub-{@link LoadProfile}s by adding up {@link TimeEstimate}s of the same type and otherwise using
     * the given objects to do the conversion.
     */
    public static LoadProfileToTimeConverter createDefault(LoadToTimeConverter cpuConverter,
                                                           LoadToTimeConverter diskCoverter,
                                                           LoadToTimeConverter networkConverter,
                                                           ResourceTimeEstimateAggregator aggregator) {
        return createTopLevelStretching(cpuConverter, diskCoverter, networkConverter, aggregator, 1d);
    }

    /**
     * Create an instance that adds up {@link TimeEstimate}s of given {@link LoadProfile}s including their
     * sub-{@link LoadProfile}s by adding up {@link TimeEstimate}s of the same type and otherwise using
     * the given objects to do the conversion. However, the top-level {@link LoadProfile} estimation is
     * stretched by a given factor.
     */
    public static LoadProfileToTimeConverter createTopLevelStretching(LoadToTimeConverter cpuConverter,
                                                                      LoadToTimeConverter diskCoverter,
                                                                      LoadToTimeConverter networkConverter,
                                                                      ResourceTimeEstimateAggregator aggregator,
                                                                      double stretch) {
        return new LoadProfileToTimeConverter(cpuConverter, diskCoverter, networkConverter) {

            @Override
            public TimeEstimate convert(LoadProfile loadProfile) {
                TimeEstimate cpuTime = this.sumWithSubprofiles(
                        loadProfile, LoadProfile::getCpuUsage, this.cpuConverter);
                TimeEstimate diskTime = this.sumWithSubprofiles(
                        loadProfile, LoadProfile::getDiskUsage, this.diskCoverter);
                TimeEstimate networkTime = this.sumWithSubprofiles(
                        loadProfile, LoadProfile::getNetworkUsage, this.networkConverter);

                TimeEstimate aggregate = aggregator.aggregate(cpuTime, diskTime, networkTime);
                aggregate = aggregate
                        .times(1 / loadProfile.getResourceUtilization())
                        .plus(new TimeEstimate(loadProfile.getOverheadMillis()));
                return aggregate;
            }

            private TimeEstimate sumWithSubprofiles(LoadProfile profile,
                                                    Function<LoadProfile, LoadEstimate> property,
                                                    LoadToTimeConverter converter) {
                final LoadEstimate topLevelPropertyValue = property.apply(profile);
                final TimeEstimate topLevelEstimate = topLevelPropertyValue == null ?
                        TimeEstimate.ZERO :
                        converter.convert(topLevelPropertyValue).times(stretch);
                return profile.getSubprofiles().stream()
                        .map(property)
                        .filter(Objects::nonNull)
                        .map(converter::convert)
                        .reduce(topLevelEstimate, TimeEstimate::plus);
            }

        };


    }

    @FunctionalInterface
    public interface ResourceTimeEstimateAggregator {

        TimeEstimate aggregate(TimeEstimate cpuEstimate, TimeEstimate diskEstimate, TimeEstimate networkEstimate);

    }

}
