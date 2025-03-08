package org.components.criterion;

public class EpochsCriterion extends Criterion {
    public EpochsCriterion(int maxEpochs) {
        super(currentValues -> (int) currentValues.getOrDefault("epoch", 0) >= maxEpochs);
    }
}
