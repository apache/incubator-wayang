package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Context for the optimization of {@link LoadProfileEstimator}s.
 */
public class Individual {

    /**
     * Orders {@link Individual}s by their fitness descendingly.
     */
    public static Comparator<Individual> fitnessComparator =
            (i1, i2) -> Double.compare(i2.getFitness(), i1.getFitness());

    private final double[] genome;

    private double fitness = Double.NaN;

    Individual(int genomeSize) {
        this.genome = new double[genomeSize];
    }

    public double[] getGenome() {
        return this.genome;
    }

    public Individual mutate(Random random, int[] activatedGenes, OptimizationSpace optimizationSpace, double mutationProb, double resetProb) {
        Individual mutant = new Individual(this.genome.length);
        double[] genome = mutant.getGenome();
        for (int i : activatedGenes) {
            final double uniform = random.nextDouble();
            if (uniform <= mutationProb) {
                final double mutatedGene = optimizationSpace.getVariable(i).mutate(this.genome[i], random);
                genome[i] = mutatedGene;
            } else if (uniform <= mutationProb + resetProb) {
                genome[i] = optimizationSpace.getVariable(i).createRandomValue(random);
            } else {
                genome[i] = this.genome[i];
            }
        }
        return mutant;
    }

    public Individual crossOver(Individual that, Random random) {
        Individual offspring = new Individual(this.genome.length);
        for (int i = 0; i < this.genome.length; i++) {
            if (random.nextDouble() < 0.5) {
                offspring.genome[i] = this.genome[i];
            } else {
                offspring.genome[i] = that.genome[i];
            }
        }
        return offspring;
    }

    /**
     * Calculate and cache the fitness of this instance, which should be a positive value.
     *
     * @param partialExecutions on which this instance should be evaluated
     * @param estimators        the {@link LoadProfileEstimator}s being configured by this instance
     * @param configuration     the {@link Configuration}
     * @see #getFitness()
     */
    public void calculateFitness(List<PartialExecution> partialExecutions,
                                 Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                                 Configuration configuration) {
        this.fitness = 0d;
        for (PartialExecution partialExecution : partialExecutions) {
            TimeEstimate timeEstimate = this.estimateTime(partialExecution, estimators, configuration);
            double partialFitness = this.calculateAbsolutePartialFitness(timeEstimate, partialExecution.getMeasuredExecutionTime());
            this.fitness += partialFitness;
        }
    }

    public double getFitness() {
        if (Double.isNaN(this.fitness)) {
            throw new IllegalStateException("The fitness of the individual has not yet been calculated.");
        }
        return this.fitness;
    }

    private double calculateRelativePartialFitness(TimeEstimate timeEstimate, long actualTime) {
        final long smoothing = 1000L;
        final long meanEstimate = timeEstimate.getGeometricMeanEstimate() + smoothing;
        actualTime = actualTime + smoothing;
        if (meanEstimate > actualTime) {
            return actualTime / (double) meanEstimate;
        } else {
            return meanEstimate / (double) actualTime;
        }
    }

    private double calculateAbsolutePartialFitness(TimeEstimate timeEstimate, long actualTime) {
        final long meanEstimate = timeEstimate.getGeometricMeanEstimate();
        final long delta = Math.abs(meanEstimate - actualTime);
        return -delta;
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
