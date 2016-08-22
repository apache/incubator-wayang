package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
 * {@link ExecutionLog}.
 */
public class GeneticOptimizerApp {

    /**
     * {@link Configuration} to be used.
     */
    private final Configuration configuration;

    /**
     * Maintains {@link Variable}s to be optimized.
     */
    private OptimizationSpace optimizationSpace;

    /**
     * Maintains {@link PartialExecution}s as training data.
     */
    private List<PartialExecution> partialExecutions;

    /**
     * Maintains a {@link LoadProfileEstimator} for every type of {@link ExecutionOperator} in the
     * {@link #partialExecutions}.
     */
    private Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators;

    public GeneticOptimizerApp(Configuration configuration) {
        this.configuration = configuration;

        // Load the ExecutionLog.
        final double samplingFactor = .1d;
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            this.partialExecutions = executionLog.stream()
                    .filter(x -> new Random().nextDouble() < samplingFactor)
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RheemException("Could not evaluate execution log.", e);
        }

        // Initialize the optimization space with its LoadProfileEstimators and associated Variables.
        this.optimizationSpace = new OptimizationSpace();
        this.estimators = new HashMap<>();
        Map<Set<Class<? extends ExecutionOperator>>, List<PartialExecution>> partialExecutionClasses = new HashMap<>();
        for (PartialExecution partialExecution : this.partialExecutions) {

            // Index the PartialExecution by its ExecutionOperators.
            final Set<Class<? extends ExecutionOperator>> execOpClasses = getExecutionOperatorClasses(partialExecution);
            partialExecutionClasses
                    .computeIfAbsent(execOpClasses, key -> new LinkedList<>())
                    .add(partialExecution);

            // Initialize an LoadProfileEstimator for each of the ExecutionOperators.
            for (PartialExecution.OperatorExecution execution : partialExecution.getOperatorExecutions()) {
                estimators.computeIfAbsent(
                        execution.getOperator().getClass(),
                        key -> DynamicLoadProfileEstimators.createLinearEstimator(execution.getOperator(), this.optimizationSpace)
                );
            }
        }

        System.out.printf(
                "Loaded %d execution records with %d execution operator types.\n",
                this.partialExecutions.size(), estimators.keySet().size()
        );
    }

    private void run() {
        // Get execution groups.
        List<List<PartialExecution>> executionGroups = this.groupPartialExecutions(this.partialExecutions).entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e1.getKey().size(), e2.getKey().size()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());


        // Create the root optimizer and an initial population.
        GeneticOptimizer generalOptimizer = new GeneticOptimizer(
                this.optimizationSpace, this.partialExecutions, estimators, this.configuration
        );
        List<Individual> population = generalOptimizer.createInitialPopulation();
        int generation = 0;

        int maxGen = 100000;
        int maxStableGen = 100;
        double minFitness = .5;

        // Optimize on samples.
        for (List<PartialExecution> group : executionGroups) {
            final Tuple<Integer, List<Individual>> newGeneration = this.superOptimize(10, population, group, generation, maxGen, maxStableGen, minFitness);
            generation = newGeneration.getField0();
            population = newGeneration.getField1();
        }

        // Optimize on the complete training data.
        final Tuple<Integer, List<Individual>> newGeneration = this.optimize(population, generalOptimizer, generation, maxGen, maxStableGen, minFitness);
        generation = newGeneration.getField0();
        population = newGeneration.getField1();
        Individual fittestIndividual = population.get(0);

        // Print the training data vs. the estimates.
        System.out.println();
        System.out.println("Training data vs. measured");
        System.out.println("==========================");
        this.partialExecutions.sort((e1, e2) -> Long.compare(e2.getMeasuredExecutionTime(), e1.getMeasuredExecutionTime()));
        for (PartialExecution partialExecution : this.partialExecutions) {
            final TimeEstimate timeEstimate = fittestIndividual.estimateTime(partialExecution, estimators, this.configuration);
            System.out.printf("Actual %s;\testimated: %s (%d operators)\n",
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    timeEstimate,
                    partialExecution.getOperatorExecutions().size());
        }

        System.out.println();
        System.out.println("Configuration file");
        System.out.println("==================");
        for (LoadProfileEstimator<Individual> estimator : estimators.values()) {
            if (estimator instanceof DynamicLoadProfileEstimator) {
                System.out.println(((DynamicLoadProfileEstimator) estimator).toJsonConfig(fittestIndividual));
            }
        }
    }

    private Tuple<Integer, List<Individual>> superOptimize(
            int numTribes,
            List<Individual> individuals,
            Collection<PartialExecution> partialExecutions,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness) {

        int individualsPerTribe = (individuals.size() + numTribes - 1) / numTribes;
        List<Individual> superpopulation = new ArrayList<>(individuals.size() * numTribes);
        int maxGeneration = 0;
        for (int i = 0; i < numTribes; i++) {
            final Tuple<Integer, List<Individual>> population = this.optimize(
                    individuals, partialExecutions, currentGeneration, maxGenerations, maxStableGenerations, minFitness
            );
            maxGeneration = Math.max(maxGeneration, population.getField0());
            superpopulation.addAll(population.getField1().subList(0, individualsPerTribe));
        }
        superpopulation.sort(Individual.fitnessComparator);
        return new Tuple<>(maxGeneration, superpopulation.subList(0, individuals.size()));
    }

    private Tuple<Integer, List<Individual>> optimize(
            List<Individual> individuals,
            Collection<PartialExecution> partialExecutions,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness) {
        GeneticOptimizer optimizer = new GeneticOptimizer(
                this.optimizationSpace, partialExecutions, this.estimators, this.configuration
        );
        return this.optimize(individuals, optimizer, currentGeneration, maxGenerations, maxStableGenerations, minFitness);
    }

    private Tuple<Integer, List<Individual>> optimize(
            List<Individual> individuals,
            GeneticOptimizer optimizer,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness) {
        System.out.printf("Optimizing %d variables on %d partial executions (e.g., %s).\n",
                optimizer.getActivatedGenes().cardinality(),
                optimizer.getData().size(),
                RheemCollections.getAny(optimizer.getData()).getOperatorExecutions()
        );

        optimizer.updateFitness(individuals);
        double checkpointFitness = Double.NEGATIVE_INFINITY;
        int i;
        for (i = 0; i < maxGenerations; i++, currentGeneration++) {
            individuals = optimizer.evolve(individuals);

            if (i % maxStableGenerations == 0) {
                System.out.printf(
                        "Fittest individual of generation %,d (%,d): %,.4f\n",
                        i,
                        currentGeneration,
                        individuals.get(0).getFitness()
                );
            }

            // Check whether we seem to be stuck in a (local) optimum.
            if (i % maxStableGenerations == 0) {
                final double bestFitness = individuals.get(0).getFitness();
                if (checkpointFitness >= bestFitness && bestFitness >= minFitness) {
                    break;
                } else {
                    checkpointFitness = bestFitness;
                }
            }
        }

        System.out.printf(
                "Final fittest individual of generation %,d (%,d): %,.4f\n",
                i,
                currentGeneration,
                individuals.get(0).getFitness()
        );

        return new Tuple<>(currentGeneration, individuals);
    }

    /**
     * Group {@link PartialExecution}s by their comprised {@link ExecutionOperator}s.
     *
     * @param partialExecutions the {@link PartialExecution}s
     * @return the grouping of the {@link #partialExecutions}
     */
    private Map<Set<Class<? extends ExecutionOperator>>, List<PartialExecution>> groupPartialExecutions(
            Collection<PartialExecution> partialExecutions) {
        Map<Set<Class<? extends ExecutionOperator>>, List<PartialExecution>> groups = new HashMap<>();
        for (PartialExecution partialExecution : partialExecutions) {

            // Determine the ExecutionOperator classes in the partialExecution.
            final Set<Class<? extends ExecutionOperator>> execOpClasses = getExecutionOperatorClasses(partialExecution);

            // Index the partialExecution.
            groups.computeIfAbsent(execOpClasses, key -> new LinkedList<>())
                    .add(partialExecution);
        }

        return groups;
    }

    /**
     * Extract the {@link ExecutionOperator} {@link Class}es in the given {@link PartialExecution}.
     *
     * @param partialExecution the {@link PartialExecution}
     * @return the {@link ExecutionOperator} {@link Class}es
     */
    private Set<Class<? extends ExecutionOperator>> getExecutionOperatorClasses(PartialExecution partialExecution) {
        return partialExecution.getOperatorExecutions().stream()
                .map(PartialExecution.OperatorExecution::getOperator)
                .map(ExecutionOperator::getClass)
                .collect(Collectors.toSet());
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        new GeneticOptimizerApp(configuration).run();
    }
}
