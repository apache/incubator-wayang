package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfile;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.util.Bitmask;

import java.util.*;

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

    private final double[] maturity;

    private double minMaturity = Double.NaN, maxMaturity = Double.NaN;

    private double fitness = Double.NaN;

    Individual(int genomeSize) {
        this.genome = new double[genomeSize];
        this.maturity = new double[genomeSize];
    }

    public double[] getGenome() {
        return this.genome;
    }

    public void setGene(int index, double value, double maturity) {
        this.genome[index] = value;
        this.updateMaturity(index, maturity);
    }

    private void updateMaturity(int index, double maturity) {
        this.maturity[index] = maturity;
        if (Double.isNaN(this.minMaturity) || this.minMaturity > maturity) {
            this.minMaturity = maturity;
        }
        if (Double.isNaN(this.maxMaturity) || this.maxMaturity < maturity) {
            this.maxMaturity = maturity;
        }
    }

    public Individual mutate(Random random,
                             Bitmask activatedGenes,
                             OptimizationSpace optimizationSpace,
                             double mutationProb,
                             double resetProb) {

        final double smoothing = 1d;
        int numActivatedGenes = activatedGenes.cardinality();
        double logGainProduct = 0d;
        for (int i = activatedGenes.nextSetBit(0); i != -1; i = activatedGenes.nextSetBit(i + 1)) {
            final double gain = this.maturity[i] - this.minMaturity;
            logGainProduct += Math.log(gain + smoothing);
        }
        double meanGain = Math.exp((logGainProduct / numActivatedGenes)) - smoothing;

        Individual mutant = new Individual(this.genome.length);
        for (int i = 0; i < this.genome.length; i++) {
            if (!activatedGenes.get(i)) {
                mutant.setGene(i, this.genome[i], this.maturity[i]);
                continue;
            }
            final double gain = this.maturity[i] - this.minMaturity;
            double boost = (meanGain + smoothing) / (gain + smoothing);
            final double uniform = random.nextDouble() * boost;
            if (uniform <= mutationProb) {
                final double mutatedGene = optimizationSpace.getVariable(i).mutate(this.genome[i], random);
                mutant.setGene(i, mutatedGene, Double.NaN);
            } else if (uniform <= mutationProb + resetProb) {
                mutant.setGene(i, optimizationSpace.getVariable(i).createRandomValue(random), Double.NaN);
            } else {
                mutant.setGene(i, this.genome[i], this.maturity[i]);
            }
        }
        return mutant;
    }

    public Individual crossOver(Individual that, Random random) {
        Individual offspring = new Individual(this.genome.length);
        double minMaturity = Math.min(this.minMaturity, that.minMaturity);
        for (int i = 0; i < this.genome.length; i++) {
            double thisProb = GeneticOptimizer.getSelectionProbability(this.maturity[i], that.maturity[i], minMaturity);
            if (random.nextDouble() < thisProb) {
                offspring.setGene(i, this.genome[i], this.maturity[i]);
            } else {
                offspring.setGene(i, that.genome[i], that.maturity[i]);
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
    public void calculateFitness(Collection<PartialExecution> partialExecutions,
                                 Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                                 Configuration configuration) {
        this.fitness = 0d;
        for (PartialExecution partialExecution : partialExecutions) {
            TimeEstimate timeEstimate = this.estimateTime(partialExecution, estimators, configuration);
            double partialFitness = this.calculateAbsolutePartialFitness(timeEstimate, partialExecution.getMeasuredExecutionTime());
            this.fitness += partialFitness;
        }
        this.fitness /= partialExecutions.size();
    }

    public void updateMaturity(Bitmask activatedGenes) {
        final double newMaturity = this.getFitness();
        for (int activatedGene = activatedGenes.nextSetBit(0);
             activatedGene != -1;
             activatedGene = activatedGenes.nextSetBit(activatedGene + 1)) {
            double currentMaturity = this.maturity[activatedGene];
            if (Double.isNaN(currentMaturity) || newMaturity > currentMaturity) {
                this.updateMaturity(activatedGene, newMaturity);
            }
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
