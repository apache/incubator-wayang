package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.util.Bitmask;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
 * {@link ExecutionLog}.
 */
public class GeneticOptimizerApp {

    /**
     * {@link Configuration} to be used.
     */
    final Configuration configuration;

    /**
     * Maintains {@link Variable}s to be optimized.
     */
    OptimizationSpace optimizationSpace;

    /**
     * Maintains {@link PartialExecution}s as training data.
     */
    List<PartialExecution> partialExecutions;

    /**
     * Maintains a {@link LoadProfileEstimator} for every type of {@link ExecutionOperator} in the
     * {@link #partialExecutions}.
     */
    Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators;

    /**
     * Maintains variables that quantify the overhead for initializing a {@link Platform}.
     */
    Map<Platform, Variable> platformOverheads = new HashMap<>();

    /**
     * Creates a new instance.
     *
     * @param configuration provides, amongst others, platform specifications
     */
    public GeneticOptimizerApp(Configuration configuration) {
        this.configuration = configuration;

        // Load the ExecutionLog.
        final double samplingFactor = 1d;
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
        this.platformOverheads = new HashMap<>();

        Map<Set<Class<? extends ExecutionOperator>>, List<PartialExecution>> partialExecutionClasses = new HashMap<>();
        for (PartialExecution partialExecution : this.partialExecutions) {

            // Index the PartialExecution by its ExecutionOperators.
            final Set<Class<? extends ExecutionOperator>> execOpClasses = getExecutionOperatorClasses(partialExecution);
            partialExecutionClasses
                    .computeIfAbsent(execOpClasses, key -> new LinkedList<>())
                    .add(partialExecution);

            // Initialize an LoadProfileEstimator for each of the ExecutionOperators.
            for (PartialExecution.OperatorExecution execution : partialExecution.getOperatorExecutions()) {
                this.estimators.computeIfAbsent(
                        execution.getOperator().getClass(),
                        key -> DynamicLoadProfileEstimators.createSuitableEstimator(
                                execution.getOperator(),
                                this.optimizationSpace,
                                this.configuration
                        )
                );
            }

            for (Platform platform : partialExecution.getInitializedPlatforms()) {
                this.platformOverheads.computeIfAbsent(
                        platform,
                        key -> this.optimizationSpace.getOrCreateVariable(key.getClass().getCanonicalName() + "->overhead")
                );
            }
        }

        System.out.printf(
                "Loaded %d execution records with %d execution operator types and %d platform overheads.\n",
                this.partialExecutions.size(), estimators.keySet().size(), this.platformOverheads.size()
        );
    }

    private void run() {
        // Get execution groups.
        List<List<PartialExecution>> executionGroups = this.groupPartialExecutions(this.partialExecutions).entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e1.getKey().size(), e2.getKey().size()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());


        // Create the root optimizer and an initial population.
        GeneticOptimizer generalOptimizer = this.createOptimizer(this.partialExecutions);
        List<Individual> population = generalOptimizer.createInitialPopulation();
        int generation = 0;

        int maxGen = (int) this.configuration.getLongProperty("rheem.profiler.ga.maxgenerations", 5000);
        int maxStableGen = (int) this.configuration.getLongProperty("rheem.profiler.ga.maxstablegenerations", 2000);
        double minFitness = this.configuration.getDoubleProperty("rheem.profiler.ga.minfitness", .9d);
        int superOptimizations = (int) this.configuration.getLongProperty("rheem.profiler.ga.superoptimizations", 3);

        // Optimize on blocks.
        if (this.configuration.getBooleanProperty("rheem.profiler.ga.block", false)) {
            for (List<PartialExecution> group : executionGroups) {
                final Collection<PartialExecution> reducedGroup = this.reduceByExecutionTime(group, 1.5);
                final PartialExecution representative = RheemCollections.getAny(reducedGroup);
                final List<String> subjects = Stream.concat(
                        representative.getOperatorExecutions().stream().map(operatorExecution -> operatorExecution.getOperator().getClass().getSimpleName()),
                        representative.getInitializedPlatforms().stream().map(Platform::getName)
                ).collect(Collectors.toList());
                if (reducedGroup.size() < 2) {
                    System.out.printf("Few measurement points for %s\n", subjects);
                }
                if (representative.getOperatorExecutions().size() > 3) {
                    System.out.printf("Many subjects for %s\n", subjects);
                }

                long minExecTime = reducedGroup.stream().mapToLong(PartialExecution::getMeasuredExecutionTime).min().getAsLong();
                long maxExecTime = reducedGroup.stream().mapToLong(PartialExecution::getMeasuredExecutionTime).max().getAsLong();
                if (maxExecTime - minExecTime < 1000) {
                    System.out.printf("Narrow training data for %s\n", subjects);
                    continue;
                }

                final Tuple<Integer, List<Individual>> newGeneration = this.superOptimize(
                        superOptimizations, population, reducedGroup, generation, maxGen, maxStableGen, minFitness
                );
                generation = newGeneration.getField0();
                population = newGeneration.getField1();

                final GeneticOptimizer tempOptimizer = this.createOptimizer(reducedGroup);
                this.printResults(tempOptimizer, population.get(0));
            }
        }

        // Optimize on the complete training data.
        final Tuple<Integer, List<Individual>> newGeneration = this.optimize(
                population, generalOptimizer, generation, maxGen, maxStableGen, minFitness
        );
        generation = newGeneration.getField0();
        population = newGeneration.getField1();
        Individual fittestIndividual = population.get(0);
        printResults(generalOptimizer, fittestIndividual);
    }

    private void printResults(GeneticOptimizer optimizer, Individual individual) {
        // Print the training data vs. the estimates.
        System.out.println();
        System.out.printf("=== Stats for fittest individual (fitness=%,.4f)\n", individual.getFitness());
        System.out.println();
        System.out.println("Training data vs. measured");
        System.out.println("==========================");
        List<PartialExecution> data = new ArrayList<>(optimizer.getData());
        data.sort((e1, e2) -> Long.compare(e2.getMeasuredExecutionTime(), e1.getMeasuredExecutionTime()));
        for (PartialExecution partialExecution : data) {
            final TimeEstimate timeEstimate = individual.estimateTime(partialExecution, this.estimators, this.platformOverheads, this.configuration);
            System.out.printf("Actual %13s | Estimated: %72s | %3d operators | %s\n",
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    timeEstimate,
                    partialExecution.getOperatorExecutions().size(),
                    Stream.concat(
                            partialExecution.getOperatorExecutions().stream().map(operatorExecution -> operatorExecution.getOperator().getClass().getSimpleName()),
                            partialExecution.getInitializedPlatforms().stream().map(Platform::getName)
                    ).collect(Collectors.toList())
            );
        }

        System.out.println();
        System.out.println("Configuration file");
        System.out.println("==================");
        final Bitmask genes = optimizer.getActivatedGenes();
        Set<Variable> optimizedVariables = new HashSet<>(genes.cardinality());
        for (int gene = genes.nextSetBit(0); gene != -1; gene = genes.nextSetBit(gene + 1)) {
            optimizedVariables.add(this.optimizationSpace.getVariable(gene));
        }
        for (Map.Entry<Platform, Variable> entry : this.platformOverheads.entrySet()) {
            final Platform platform = entry.getKey();
            final Variable overhead = entry.getValue();
            if (!optimizedVariables.contains(overhead)) continue;
            System.out.printf("(overhead of %s) = %d\n",
                    platform.getName(),
                    Math.round(overhead.getValue(individual))
            );
        }
        for (LoadProfileEstimator<Individual> estimator : estimators.values()) {
            if (estimator instanceof DynamicLoadProfileEstimator) {
                final DynamicLoadProfileEstimator dynamicLoadProfileEstimator = (DynamicLoadProfileEstimator) estimator;
                if (!optimizedVariables.containsAll(dynamicLoadProfileEstimator.getEmployedVariables())) continue;
                System.out.println(dynamicLoadProfileEstimator.toJsonConfig(individual));
            }
        }
    }

    /**
     * Creates a new {@link GeneticOptimizer} that used the given {@link PartialExecution}s as training data.
     *
     * @param partialExecutions the training data
     * @return the {@link GeneticOptimizer}
     */
    private GeneticOptimizer createOptimizer(Collection<PartialExecution> partialExecutions) {
        return new GeneticOptimizer(this.optimizationSpace, partialExecutions, this.estimators, this.platformOverheads, this.configuration);
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
        GeneticOptimizer optimizer = this.createOptimizer(partialExecutions);
        return this.optimize(individuals, optimizer, currentGeneration, maxGenerations, maxStableGenerations, minFitness);
    }

    private Tuple<Integer, List<Individual>> optimize(
            List<Individual> individuals,
            GeneticOptimizer optimizer,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness) {

        int updateFrequency = (int) this.configuration.getLongProperty("rheem.profiler.ga.intermediateupate", -1);
        System.out.printf("Optimizing %d variables on %d partial executions (e.g., %s).\n",
                optimizer.getActivatedGenes().cardinality(),
                optimizer.getData().size(),
                RheemCollections.getAny(optimizer.getData()).getOperatorExecutions()
        );

        optimizer.updateFitness(individuals);
        double checkpointFitness = Double.NEGATIVE_INFINITY;
        int i;
        for (i = 0; i < maxGenerations; i++, currentGeneration++) {
            // Print status.
            if (i % maxStableGenerations == 0) {
                System.out.printf(
                        "Fittest individual of generation %,d (%,d): %,.4f\n",
                        i,
                        currentGeneration,
                        individuals.get(0).getFitness()
                );
            }

            individuals = optimizer.evolve(individuals);

            if (updateFrequency > 0 && i > 0 && updateFrequency % i == 0) {
                this.printResults(optimizer, individuals.get(0));
            }

            // Check whether we seem to be stuck in a (local) optimum.
            if (i % maxStableGenerations == 0) {
                final double bestFitness = individuals.get(0).getFitness();
                if (checkpointFitness >= bestFitness && bestFitness >= minFitness && i > 0) {
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

    private Collection<PartialExecution> reduceByExecutionTime(Collection<PartialExecution> partialExecutions, double densityFactor) {
        Map<Integer, PartialExecution> resultBins = new HashMap<>();
        for (PartialExecution partialExecution : partialExecutions) {
            int key = (int) Math.round(Math.log1p(partialExecution.getMeasuredExecutionTime()) / Math.log(densityFactor));
            resultBins.put(key, partialExecution);
        }
        return resultBins.values();
    }

    public static void main(String[] args) {
        Configuration configuration = args.length == 0 ? new Configuration() : new Configuration(args[0]);
        new GeneticOptimizerApp(configuration).run();
    }
}
