package org.qcri.rheem.profiler.log;

/**
 * TODO
 */
public class DynamicLoadProfileEstimatorAdapter extends DynamicLoadProfileEstimator {

    public DynamicLoadProfileEstimatorAdapter(String configKey, int numInputs, int numOutputs, DynamicLoadEstimator cpuEstimator) {
        super(configKey, numInputs, numOutputs, cpuEstimator);
    }
}
