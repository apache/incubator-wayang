package org.qcri.rheem.core.optimizer.costs;

/**
 * Converts a {@link LoadEstimate} into a time estimate.
 */
public abstract class LoadToTimeConverter {

    public abstract TimeEstimate convert(LoadEstimate loadEstimate);

    /**
     * Create linear converter.
     *
     * @param millisPerUnit coefficient: number of milliseconds per resource usage unit
     * @return the new instance
     */
    public static LoadToTimeConverter createLinearCoverter(final double millisPerUnit) {
        return new LoadToTimeConverter() {
            @Override
            public TimeEstimate convert(LoadEstimate loadEstimate) {
                return new TimeEstimate(
                        (long) Math.ceil(millisPerUnit * loadEstimate.getLowerEstimate()),
                        (long) Math.ceil(millisPerUnit * loadEstimate.getUpperEstimate()),
                        loadEstimate.getCorrectnessProbability()
                );
            }

            @Override
            public String toString() {
                return String.format("LoadToTimeConverter[%,.2f units/ms]", 1d / millisPerUnit);
            }
        };
    }

}
