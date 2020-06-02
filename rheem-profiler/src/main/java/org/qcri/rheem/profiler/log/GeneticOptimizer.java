package org.qcri.rheem.profiler.log;

import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.platform.AtomicExecution;
import org.qcri.rheem.core.platform.AtomicExecutionGroup;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.Bitmask;
import org.qcri.rheem.profiler.log.sampling.Sampler;
import org.qcri.rheem.profiler.log.sampling.TournamentSampler;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.ToDoubleFunction;

/**
 * Implementation of the genetic optimization technique for finding good {@link LoadProfileEstimator}s.
 */
public class GeneticOptimizer {

    /**
     * {@link Configuration} to be used in the estimation process.
     */
    private final Configuration configuration;

    /**
     * Represents the {@link Variable}s to be optimized.
     */
    private final OptimizationSpace optimizationSpace;

    /**
     * Observations to assess the fitness of the to-be-learnt function.
     */
    private final Collection<PartialExecution> observations;

    /**
     * Counts observation instances, such as an operator or a platform initialization, in the training data.
     */
    private final TObjectIntMap<Object> numObservations;

    /**
     * {@link Variable}s to learn the overhead of {@link Platform} initialization.
     */
    private final Map<Platform, Variable> platformOverheads;

    /**
     * Size of the population and its elite.
     */
    private final int populationSize, eliteSize;

    /**
     * Ratio of {@link Individual}s of a new generation that should be conceived via mutation.
     */
    private final double selectionRatio;

    /**
     * Ratio of {@link Individual}s of a new generation that should be conceived via mutation.
     */
    private final double mutationRatio;

    /**
     * Ratio of genes that should be altered in a mutation.
     */
    private final double mutationAlterationRatio;

    /**
     * Ratio of genes that should be generated completely freely in a mutation.
     */
    private final double mutationResetRatio;

    /**
     * Indices into the genome of {@link Individual}s that should be optimized.
     */
    private final Bitmask activatedGenes;

    /**
     * Provides randomness to the optimization.
     */
    private final Random random = new Random();

    /**
     * Fitness function for assessing {@link Individual}s.
     */
    private final ToDoubleFunction<Individual> fitnessFunction;

    /**
     * The sum of the runtime of all {@link #observations}.
     */
    private long runtimeSum;

    /**
     * Creates a new instance.
     */
    public GeneticOptimizer(OptimizationSpace optimizationSpace,
                            Collection<PartialExecution> observations,
                            Map<String, DynamicLoadProfileEstimator> estimators,
                            Map<Platform, Variable> platformOverheads,
                            Configuration configuration) {
        this.configuration = configuration;
        this.optimizationSpace = optimizationSpace;
        this.observations = observations;
        this.platformOverheads = platformOverheads;
        this.activatedGenes = new Bitmask(this.optimizationSpace.getNumDimensions());
        for (PartialExecution observation : observations) {
            final Collection<String> loadProfileEstimatorKeys = getLoadProfileEstimatorKeys(observation);
            for (String loadProfileEstimatorKey : loadProfileEstimatorKeys) {
                final LoadProfileEstimator estimator = estimators.get(loadProfileEstimatorKey);
                if (estimator != null) {
                    for (Variable variable : ((DynamicLoadProfileEstimator) estimator).getEmployedVariables()) {
                        this.activatedGenes.set(variable.getIndex());
                    }
                }
            }
            for (Variable platformOverhead : this.platformOverheads.values()) {
                this.activatedGenes.set(platformOverhead.getIndex());
            }
        }

        this.populationSize = ((int) this.configuration.getLongProperty("rheem.profiler.ga.population.size", 10));
        this.eliteSize = ((int) this.configuration.getLongProperty("rheem.profiler.ga.population.elite", 1));
        this.selectionRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.selection.ratio", 0.5d);
        this.mutationRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.ratio", 0.5d);
        this.mutationAlterationRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.alteration", 0.5d);
        this.mutationResetRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.reset", 0.01d);
        switch (this.configuration.getStringProperty("rheem.profiler.ga.fitness.type", "relative")) {
            case "relative":
                this.fitnessFunction = individual -> individual.calculateRelativeFitness(this);
                break;
            case "absolute":
                this.fitnessFunction = individual -> individual.calculateAbsoluteFitness(this);
                break;
//            case "subject":
//                this.fitnessFunction = individual -> individual.calcluateSubjectbasedFitness(this);
//                break;
            default:
                throw new IllegalStateException(
                        "Unknown fitness function: " + this.configuration.getStringProperty("rheem.profiler.ga.fitness.type")
                );
        }

        // Count the distinct elements in the PartialExecutions.
        this.numObservations = new TObjectIntHashMap<>();
        this.runtimeSum = 0L;
        for (PartialExecution observation : this.observations) {
            for (String key : getLoadProfileEstimatorKeys(observation)) {
                this.numObservations.adjustOrPutValue(key, 1, 1);
            }
            for (Platform platform : observation.getInitializedPlatforms()) {
                this.numObservations.adjustOrPutValue(platform, 1, 1);
            }
            this.runtimeSum += observation.getMeasuredExecutionTime();
        }
    }

    /**
     * Creates a population of random {@link Individual}s.
     *
     * @return the {@link Individual}s ordered by their fitness
     */
    public List<Individual> createInitialPopulation() {
        List<Individual> individuals = new ArrayList<>(this.populationSize);
        for (int i = 0; i < this.populationSize; i++) {
            final Individual individual = this.optimizationSpace.createRandomIndividual(this.random);
            this.updateFitnessOf(individual);
            individuals.add(individual);
        }
        individuals.sort(Individual.fitnessComparator);
        return individuals;
    }

    /**
     * Update the fitness of the {@link Individual}s w.r.t. to this instance and sort them according to their new fitness.
     *
     * @param individuals the {@link Individual}s
     */
    public void updateFitness(List<Individual> individuals) {
        individuals.forEach(this::updateFitnessOf);
        individuals.sort(Individual.fitnessComparator);
    }

    private void updateFitnessOf(Individual individual) {
        individual.updateFitness(this.fitnessFunction);
        individual.updateMaturity(this.activatedGenes);
    }

    public List<Individual> evolve(List<Individual> population) {
        assert population.size() == this.populationSize;
        ArrayList<Individual> nextGeneration = new ArrayList<>(this.populationSize + this.eliteSize);

        // Select individuals that should be able to propagate.
        double maxFitness = population.get(0).getFitness(), minFitness = population.get(this.populationSize - 1).getFitness();
        Sampler<Individual> selector = new TournamentSampler<>();
        final List<Individual> selectedIndividuals = selector.sample(
                population,
                (i1, i2) -> (this.random.nextDouble() < getSelectionProbability(i1.getFitness(), i2.getFitness(), minFitness)) ? i1 : i2,
                this.selectionRatio
        );
        int selectionSize = selectedIndividuals.size();

        // Create mutations.
        int numMutations = (int) Math.round(this.mutationRatio * this.populationSize);
        for (int i = 0; i < numMutations; i++) {
            final Individual individual = selectedIndividuals.get(this.random.nextInt(selectionSize));
            final Individual mutant = individual.mutate(
                    this.random, this.activatedGenes, this.optimizationSpace, this.mutationAlterationRatio, this.mutationResetRatio
            );
            this.updateFitnessOf(mutant);
            nextGeneration.add(mutant);
        }

        // Cross over.
        int numCrossOvers = this.populationSize - numMutations;
        for (int i = 0; i < numCrossOvers; i++) {
            final Individual individual1 = selectedIndividuals.get(this.random.nextInt(selectionSize));
            final Individual individual2 = selectedIndividuals.get(this.random.nextInt(selectionSize));
            final Individual offspring = individual1.crossOver(individual2, this.random);
            this.updateFitnessOf(offspring);
            nextGeneration.add(offspring);
        }

        // Process elites.
        for (int i = 0; i < this.eliteSize; i++) {
            nextGeneration.add(population.get(i));
        }
        nextGeneration.sort(Individual.fitnessComparator);
        for (int i = this.populationSize + this.eliteSize - 1; i >= this.populationSize; i--) {
            nextGeneration.remove(i);
        }

        assert nextGeneration.size() == this.populationSize;
        return nextGeneration;
    }

    public static double getSelectionProbability(double score1, double score2, double minScore) {
        if (score1 == score2) return 0.5d;
        score1 -= minScore;
        score2 -= minScore;
        return score1 / (score1 + score2);
    }

    public Bitmask getActivatedGenes() {
        return activatedGenes;
    }

    public Collection<PartialExecution> getData() {
        return this.observations;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public OptimizationSpace getOptimizationSpace() {
        return optimizationSpace;
    }

    public Collection<PartialExecution> getObservations() {
        return observations;
    }

    public TObjectIntMap<Object> getNumObservations() {
        return numObservations;
    }

    public Map<Platform, Variable> getPlatformOverheads() {
        return platformOverheads;
    }

    public double calculateObservationBasedWeight(PartialExecution observation) {
        double weight = 0;
        for (Platform platform : observation.getInitializedPlatforms()) {
            weight += 1d / this.numObservations.get(platform);
        }
        for (String key : getLoadProfileEstimatorKeys(observation)) {
            weight += 1d / this.numObservations.get(key);
        }

        return weight / this.numObservations.size();
    }

    public double calculateRuntimeBasedWeight(PartialExecution observation) {
        return observation.getMeasuredExecutionTime() / (double) this.runtimeSum;
    }

    /**
     * Collects all configuration keys of {@link LoadProfileEstimator}s embedded in the given {@link PartialExecution}.
     *
     * @param partialExecution the {@link PartialExecution}
     * @return the configuration keys
     */
    private static Collection<String> getLoadProfileEstimatorKeys(PartialExecution partialExecution) {
        Collection<String> keys = new LinkedList<>();
        for (AtomicExecutionGroup atomicExecutionGroup : partialExecution.getAtomicExecutionGroups()) {
            for (AtomicExecution atomicExecution : atomicExecutionGroup.getAtomicExecutions()) {
                keys.addAll(atomicExecution.getLoadProfileEstimator().getConfigurationKeys());
            }
        }
        return keys;
    }


}
