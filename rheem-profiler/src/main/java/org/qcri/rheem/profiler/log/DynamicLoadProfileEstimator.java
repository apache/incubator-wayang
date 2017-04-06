package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

/**
 * Adjustable {@link LoadProfileEstimator} implementation.
 */
public class DynamicLoadProfileEstimator implements LoadProfileEstimator {

    /**
     * Number of expected input/output {@link CardinalityEstimate}s.
     */
    private final int numInputs, numOutputs;

    /**
     * {@link DynamicLoadEstimator}s for the several estimated metrics.
     */
    private final DynamicLoadEstimator cpuEstimator, ramEstimator, diskEstimator, networkEstimator;

    /**
     * All {@link Variable}s employed by this instance and its {@link DynamicLoadEstimator}s.
     */
    private final Collection<Variable> employedVariables = new HashSet<>();

    private final Collection<LoadProfileEstimator> nestedEstimators = new ArrayList<>(4);

    /**
     * {@link Configuration} key of this instance.
     */
    private final String configKey;

    /**
     * Creates a new instance.
     *
     * @param numInputs    the number of input {@link CardinalityEstimate}s to the new instance
     * @param numOutputs   the number of output {@link CardinalityEstimate}s to the new instance
     * @param cpuEstimator a {@link DynamicLoadEstimator} to estimate CPU load
     */
    public DynamicLoadProfileEstimator(String configKey, int numInputs, int numOutputs, DynamicLoadEstimator cpuEstimator) {
        this(configKey, numInputs, numOutputs, cpuEstimator, DynamicLoadEstimator.zeroLoad, DynamicLoadEstimator.zeroLoad);
    }

    /**
     * Creates a new instance.
     *
     * @param configKey        with which this instance should be associated in a {@link Configuration}.
     * @param numInputs        the number of input {@link CardinalityEstimate}s to the new instance
     * @param numOutputs       the number of output {@link CardinalityEstimate}s to the new instance
     * @param cpuEstimator     a {@link DynamicLoadEstimator} to estimate CPU load
     * @param diskEstimator    a {@link DynamicLoadEstimator} to estimate disk load
     * @param networkEstimator a {@link DynamicLoadEstimator} to estimate network load
     */
    public DynamicLoadProfileEstimator(String configKey,
                                       int numInputs,
                                       int numOutputs,
                                       DynamicLoadEstimator cpuEstimator,
                                       DynamicLoadEstimator diskEstimator,
                                       DynamicLoadEstimator networkEstimator) {
        this.configKey = configKey;
        this.numInputs = numInputs;
        this.numOutputs = numOutputs;
        this.cpuEstimator = cpuEstimator;
        this.ramEstimator = DynamicLoadEstimator.zeroLoad;
        this.diskEstimator = diskEstimator;
        this.networkEstimator = networkEstimator;

        this.employedVariables.addAll(this.cpuEstimator.getEmployedVariables());
        this.employedVariables.addAll(this.ramEstimator.getEmployedVariables());
        this.employedVariables.addAll(this.diskEstimator.getEmployedVariables());
        this.employedVariables.addAll(this.networkEstimator.getEmployedVariables());
    }

    @Override
    public LoadProfile estimate(EstimationContext estimationContext) {
        return new LoadProfile(
                this.cpuEstimator.calculate(estimationContext),
                this.ramEstimator.calculate(estimationContext),
                this.diskEstimator.calculate(estimationContext),
                this.networkEstimator.calculate(estimationContext)
        );
    }

    @Override
    public void nest(LoadProfileEstimator loadProfileEstimator) {
        this.nestedEstimators.add(loadProfileEstimator);
    }

    @Override
    public Collection<LoadProfileEstimator> getNestedEstimators() {
        return this.nestedEstimators;
    }

    @Override
    public String getConfigurationKey() {
        return this.configKey;
    }

    /**
     * Creates a JSON representation of this instance that can be plugged into a properties file.
     *
     * @param individual provides parameterization of this instance
     * @return the JSON configuration representation
     */
    public String toJsonConfig(Individual individual) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.configKey).append(" = {\\\n");
        sb.append(" \"type\":\"mathex\",\\\n");
        sb.append(" \"in\":").append(this.numInputs).append(",\\\n");
        sb.append(" \"out\":").append(this.numOutputs).append(",\\\n");
        sb.append(" \"cpu\":\"").append(this.cpuEstimator.toMathEx(individual)).append("\",\\\n");
        sb.append(" \"ram\":\"").append(this.ramEstimator.toMathEx(individual)).append("\",\\\n");
        sb.append(" \"disk\":\"").append(this.diskEstimator.toMathEx(individual)).append("\",\\\n");
        sb.append(" \"net\":\"").append(this.networkEstimator.toMathEx(individual)).append("\",\\\n");
        sb.append(" \"p\":0.9\\\n");
        sb.append("}");
        return sb.toString();
    }

    public Collection<Variable> getEmployedVariables() {
        return this.employedVariables;
    }


}
