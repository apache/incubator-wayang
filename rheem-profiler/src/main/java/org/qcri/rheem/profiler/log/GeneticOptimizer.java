package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;

import java.util.*;

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
    private final List<PartialExecution> observations;

    /**
     * {@link LoadProfileEstimator}s that are relevant to calculate the fitness of {@link Individual}s.
     */
    private final Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators;

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

    private final int[] activatedGenes;

    /**
     * Provides randomness to the optimization.
     */
    private final Random random = new Random();

    /**
     * Creates a new instance.
     */
    public GeneticOptimizer(OptimizationSpace optimizationSpace,
                            List<PartialExecution> observations,
                            Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators,
                            Configuration configuration) {
        this.configuration = configuration;
        this.optimizationSpace = optimizationSpace;
        this.observations = observations;
        this.estimators = estimators;
        Set<Integer> activatedGeneIndices = new HashSet<>();
        for (PartialExecution observation : observations) {
            for (PartialExecution.OperatorExecution opExec : observation.getOperatorExecutions()) {
                final LoadProfileEstimator<Individual> estimator = estimators.get(opExec.getOperator().getClass());
                if (estimator instanceof DynamicLoadProfileEstimator) {
                    for (Variable variable : ((DynamicLoadProfileEstimator) estimator).getEmployedVariables()) {
                        activatedGeneIndices.add(variable.getIndex());
                    }
                }
            }
        }
        this.activatedGenes = new int[activatedGeneIndices.size()];
        int i = 0;
        for (Integer activatedGeneIndex : activatedGeneIndices) {
            this.activatedGenes[i] = activatedGeneIndex;
        }
        Arrays.sort(this.activatedGenes);

        this.populationSize = ((int) this.configuration.getLongProperty("rheem.profiler.ga.population.size", 10));
        this.eliteSize = ((int) this.configuration.getLongProperty("rheem.profiler.ga.population.elite", 1));
        this.selectionRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.selection.ratio", 0.5d);
        this.mutationRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.ratio", 0.5d);
        this.mutationAlterationRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.alteration", 0.5d);
        this.mutationResetRatio = this.configuration.getDoubleProperty("rheem.profiler.ga.mutation.reset", 0.01d);
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
        individual.calculateFitness(this.observations, this.estimators, this.configuration);
    }

    public List<Individual> evolve(List<Individual> population) {
        assert population.size() == this.populationSize;
        ArrayList<Individual> nextGeneration = new ArrayList<>(this.populationSize + this.eliteSize);

        // Select individuals that should be able to propagate.
        double maxFitness = population.get(0).getFitness(), minFitness = population.get(this.populationSize - 1).getFitness();
        int selectionSize = ((int) Math.ceil(this.populationSize * this.selectionRatio));
        List<Individual> selectedIndividuals = new ArrayList<>(selectionSize);
        for (int i = 0; i < selectionSize; i++) {
            Individual individual1 = population.get(i);
            double points1 = individual1.getFitness() - minFitness;
            Individual individual2 = population.get(this.random.nextInt(this.populationSize));
            double points2 = individual2.getFitness() - minFitness;
            Individual choice = this.random.nextDouble() <= points1 / (points1 + points2) ?
                    individual1 :
                    individual2;
            selectedIndividuals.add(choice);
        }

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
}
