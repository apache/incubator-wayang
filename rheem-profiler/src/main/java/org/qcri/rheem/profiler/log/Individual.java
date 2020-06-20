package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.platform.AtomicExecutionGroup;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Bitmask;

import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;

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
        maturity = 1d;
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

        // Make at least one mutation more likely.
        if (mutationProb > 0d) mutationProb = Math.max(mutationProb, 1 / activatedGenes.cardinality());

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

//    /**
//     * Calculate the fitness as the arithmetic mean the individual fitnesses of the estimation subjects.
//     */
//    double calcluateSubjectbasedFitness(GeneticOptimizer geneticOptimizer) {
//
//        Map<Object, FitnessAggregator> subjectAggregators = new HashMap<>();
//        for (PartialExecution partialExecution : geneticOptimizer.getData()) {
//            // Calculate values for the given partialExecution
//            double timeEstimate = this.estimateTime(
//                    partialExecution,
//                    geneticOptimizer.getEstimators(),
//                    geneticOptimizer.getPlatformOverheads(),
//                    geneticOptimizer.getConfiguration()
//            );
//            double partialFitness = this.calculateRelativeDelta(timeEstimate, partialExecution.getMeasuredExecutionTime());
//            double weight = Math.log(Math.max(timeEstimate, partialExecution.getMeasuredExecutionTime()) + 2d) / Math.log(2);
////            double weight = Math.max(timeEstimate.getGeometricMeanEstimate(), partialExecution.getMeasuredExecutionTime()) + 1;
//
//            // Attribute the fitness to all involved subjects.
//            for (PartialExecution.OperatorExecution operatorExecution : partialExecution.getOperatorExecutions()) {
//                Object subject = operatorExecution.getOperator().getClass();
//                final FitnessAggregator aggregator = subjectAggregators.computeIfAbsent(subject, k -> new FitnessAggregator(0, 0));
//                aggregator.fitnessAccumulator += weight * partialFitness;
//                aggregator.weightAccumulator += weight;
//                aggregator.numObservations++;
//            }
//            for (Platform subject : partialExecution.getInitializedPlatforms()) {
//                final FitnessAggregator aggregator = subjectAggregators.computeIfAbsent(subject, k -> new FitnessAggregator(0, 0));
//                aggregator.fitnessAccumulator += weight * partialFitness;
//                aggregator.weightAccumulator += weight;
//                aggregator.numObservations++;
//            }
//        }
//
//        // Aggregate the fitness values of the different subjects.
//        FitnessAggregator aggregator = new FitnessAggregator(0, 0);
//        for (FitnessAggregator subjectAggregator : subjectAggregators.values()) {
//            double subjectFitness = subjectAggregator.fitnessAccumulator / subjectAggregator.weightAccumulator;
//            double subjectWeight = 1;//Math.log(1 + subjectAggregator.numObservations);
//            aggregator.fitnessAccumulator += subjectWeight * subjectFitness;
//            aggregator.weightAccumulator += subjectWeight;
//            aggregator.numObservations++;
//        }
//
//        return aggregator.fitnessAccumulator / aggregator.weightAccumulator;
//
//    }

//    private static class FitnessAggregator {
//
//        private double fitnessAccumulator;
//
//        private double weightAccumulator;
//
//        private int numObservations = 0;
//
//        public FitnessAggregator(double fitnessAccumulator, double weightAccumulator) {
//            this.fitnessAccumulator = fitnessAccumulator;
//            this.weightAccumulator = weightAccumulator;
//        }
//    }

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

    /**
     * Update the fitness of this instance.
     *
     * @param fitnessFunction calculates the fitness for this instance
     * @return the new fitness
     */
    public double updateFitness(ToDoubleFunction<Individual> fitnessFunction) {
        return this.fitness = fitnessFunction.applyAsDouble(this);
    }

    public double getFitness() {
        if (Double.isNaN(this.fitness)) {
            throw new IllegalStateException("The fitness of the individual has not yet been calculated.");
        }
        return this.fitness;
    }

    /**
     * Calculate the fitness as weighted harmonic mean of the relative prediction accuracies.
     */
    double calculateRelativeFitness(GeneticOptimizer geneticOptimizer) {
        // Some settings.
        double harmonicSmoothing = .1d;
        double weightSum = 0d;
        double fitnessSum = 0d;

        // Calculate the arithmetic mean of the partial fitnesses for each data point.
        for (PartialExecution partialExecution : geneticOptimizer.getData()) {
            // Estimate the time with the current variables.
            double timeEstimate = this.estimateTime(
                    partialExecution,
                    geneticOptimizer.getPlatformOverheads(),
                    geneticOptimizer.getConfiguration()
            );

            // Calculate the weight.
//            double weight = Math.log(partialExecution.getMeasuredExecutionTime() + 2d) / Math.log(2);
//            double weight = Math.sqrt(Math.max(timeEstimate, partialExecution.getMeasuredExecutionTime())) + 1;
            double weight = geneticOptimizer.calculateObservationBasedWeight(partialExecution);
//                    + geneticOptimizer.calculateRuntimeBasedWeight(partialExecution);

            // Calculate the partial fitness.
            double relativeDelta = this.calculateRelativeDelta(timeEstimate, partialExecution.getMeasuredExecutionTime());

            // Prepare mean calculation.
//            fitnessSum += weight / (partialFitness + harmonicSmoothing);
            fitnessSum += weight * (relativeDelta * relativeDelta);
            weightSum += weight;
        }
//        return (weightSum / fitnessSum) - harmonicSmoothing;
        return -Math.sqrt(fitnessSum) / weightSum;
    }

    private double calculateRelativeDelta(double timeEstimate, long actualTime) {
        // Get important values.
        final double smoothing = 1000d;
        final long meanEstimate = Math.round(timeEstimate);
        final long delta = Math.abs(meanEstimate - actualTime);
        return (delta + smoothing) / (actualTime + smoothing);
    }

    /**
     * Calculate the fitness as weighted arithmetic mean of the absolute prediction accuracies.
     */
    double calculateAbsoluteFitness(GeneticOptimizer geneticOptimizer) {
        double weightSum = 0d;
        double fitnessSum = 0d;
        for (PartialExecution partialExecution : geneticOptimizer.getData()) {
            double timeEstimate = this.estimateTime(
                    partialExecution,
                    geneticOptimizer.getPlatformOverheads(),
                    geneticOptimizer.getConfiguration()
            );
            double weight = geneticOptimizer.calculateObservationBasedWeight(partialExecution)
                    + 3 * geneticOptimizer.calculateRuntimeBasedWeight(partialExecution);
            double partialFitness = this.calculateAbsolutePartialFitness(timeEstimate, partialExecution.getMeasuredExecutionTime());
            weightSum += weight;
            fitnessSum += weight * -(partialFitness * partialFitness);
        }
        return fitnessSum / weightSum;
    }

    private double calculateAbsolutePartialFitness(double timeEstimate, long actualTime) {
        final long delta = Math.abs(Math.round(timeEstimate) - actualTime);
        return -delta;
    }

    double estimateTime(PartialExecution partialExecution,
                        Map<Platform, Variable> platformOverheads,
                        Configuration configuration) {
        final DoubleStream operatorEstimates = partialExecution.getAtomicExecutionGroups().stream()
                .map(atomicExecutionGroup -> this.estimateTime(atomicExecutionGroup, configuration))
                .mapToDouble(TimeEstimate::getGeometricMeanEstimate);
        final DoubleStream platformEstimates = partialExecution.getInitializedPlatforms().stream()
                .mapToDouble(p -> {
                    final Variable variable = platformOverheads.get(p);
                    return variable == null ? 0d : variable.getValue(this);
                });
        return DoubleStream.concat(operatorEstimates, platformEstimates).sum();
    }

    /**
     * Estimates the execution time for the given {@link AtomicExecutionGroup} with the genome of this instance.
     *
     * @param executionGroup the {@link AtomicExecutionGroup}
     * @param configuration  provides estimation context
     * @return the {@link TimeEstimate}
     */
    private TimeEstimate estimateTime(AtomicExecutionGroup executionGroup,
                                      Configuration configuration) {
        final EstimationContext estimationContext = executionGroup.getEstimationContext();
        return executionGroup.estimateExecutionTime(new DynamicEstimationContext(this, estimationContext));
    }
}
