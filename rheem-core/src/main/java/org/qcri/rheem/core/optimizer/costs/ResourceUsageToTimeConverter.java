package org.qcri.rheem.core.optimizer.costs;

/**
 * Converts a {@link ResourceUsageEstimate} into a time estimate.
 */
public abstract class ResourceUsageToTimeConverter {

    public abstract TimeEstimate convert(ResourceUsageEstimate resourceUsageEstimate);

    /**
     * Create linear converter.
     *
     * @param secsPerUnit coefficient: number of seconds per resource usage unit
     * @return the new instance
     */
    public static ResourceUsageToTimeConverter createLinearCoverter(final double secsPerUnit) {
        return new ResourceUsageToTimeConverter() {
            @Override
            public TimeEstimate convert(ResourceUsageEstimate resourceUsageEstimate) {
                return new TimeEstimate(
                        (long) secsPerUnit * resourceUsageEstimate.getLowerEstimate(),
                        (long) secsPerUnit * resourceUsageEstimate.getUpperEstimate(),
                        resourceUsageEstimate.getCorrectnessProbability()
                );
            }
        };
    }

}
