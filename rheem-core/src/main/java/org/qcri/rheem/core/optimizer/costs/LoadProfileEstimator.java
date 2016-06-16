package org.qcri.rheem.core.optimizer.costs;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

/**
 * Estimates the {@link LoadProfile} of some executable artifact.
 */
public interface LoadProfileEstimator {

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param inputEstimates  the number input data quanta (possibly over multiple executions)
     * @param outputEstimates the number output data quanta (possibly over multiple executions)
     * @param numExecutions   the number of executions, should be at least {@code 1}
     * @return the {@link LoadProfile}
     */
    LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates, int numExecutions);

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param inputEstimates  the number input data quanta
     * @param outputEstimates the number output data quanta
     * @return the {@link LoadProfile}
     * @see #estimate(CardinalityEstimate[], CardinalityEstimate[], int)
     * @see #estimate(OptimizationContext.OperatorContext)
     * @deprecated and retained for testing only
     */
    default LoadProfile estimate(CardinalityEstimate[] inputEstimates, CardinalityEstimate[] outputEstimates) {
        return this.estimate(inputEstimates, outputEstimates, 1);
    }

    /**
     * Estimates a {@link LoadProfile}.
     *
     * @param operatorContext describes the execution context of a {@link ExecutionOperator} whose load should be estimated
     * @return the {@link LoadProfile}
     */
    LoadProfile estimate(OptimizationContext.OperatorContext operatorContext);

}
