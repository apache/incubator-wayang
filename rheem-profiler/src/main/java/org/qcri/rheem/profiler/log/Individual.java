package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Context for the optimization of {@link LoadProfileEstimator}s.
 */
public class Individual {

    private final double[] genome;

    Individual(int genomeSize) {
        this.genome = new double[genomeSize];
    }

    public double[] getGenome() {
        return this.genome;
    }

    public Individual mutate(Random random, OptimizationSpace optimizationSpace, double mutationProb, double resetProb) {
        Individual mutant = new Individual(this.genome.length);
        double[] genome = mutant.getGenome();
        for (int i = 0; i < genome.length; i++) {
            final double uniform = random.nextDouble();
            genome[i] = uniform <= mutationProb ?
                    optimizationSpace.getVariable(i).mutate(this.genome[i], random) :
                    uniform <= mutationProb + resetProb ?
                            optimizationSpace.getVariable(i).createRandomValue(random) :
                            this.genome[i];
        }
        return mutant;
    }

    public double calculateFitness(List<PartialExecution> partialExecutions,
                                   Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                                   Configuration configuration) {
        double fitness = 0d;
        for (PartialExecution partialExecution : partialExecutions) {
            TimeEstimate timeEstimate = this.estimateTime(partialExecution, estimators, configuration);
            double partialFitness = this.calculatePartialFitness(timeEstimate, partialExecution.getMeasuredExecutionTime());
            fitness += partialFitness;
        }
        return fitness / partialExecutions.size();
    }

    private double calculatePartialFitness(TimeEstimate timeEstimate, long actualTime) {
        final long smoothing = 1000L;
        final long meanEstimate = timeEstimate.getGeometricMeanEstimate() + smoothing;
        actualTime = actualTime + smoothing;
        if (meanEstimate > actualTime) {
            return actualTime / (double) meanEstimate;
        } else {
            return meanEstimate / (double) actualTime;
        }
    }

    TimeEstimate estimateTime(PartialExecution partialExecution,
                              Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                              Configuration configuration) {
        return partialExecution.getOperatorExecutions().stream()
                .map(operatorExecution -> this.estimateTime(operatorExecution, estimators, configuration))
                .reduce(TimeEstimate.ZERO, TimeEstimate::plus);
    }

    private TimeEstimate estimateTime(PartialExecution.OperatorExecution operatorExecution,
                                      Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                                      Configuration configuration) {
        final ExecutionOperator operator = operatorExecution.getOperator();
        final LoadProfileEstimator<Individual> estimator = estimators.get(operator.getClass());
        final LoadProfile loadProfile = estimator.estimate(
                this, operatorExecution.getInputCardinalities(), operatorExecution.getOutputCardinalities()
        );
        final LoadProfileToTimeConverter timeConverter = configuration
                .getLoadProfileToTimeConverterProvider()
                .provideFor(operator.getPlatform());
        return timeConverter.convert(loadProfile);
    }
}
