package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.EstimationContext;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.AtomicExecution;
import org.qcri.rheem.core.platform.AtomicExecutionGroup;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.util.Bitmask;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.graphchi.GraphChi;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.postgres.Postgres;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.sqlite3.Sqlite3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
 * {@link ExecutionLog}.
 */
public class GeneticOptimizerApp {

    private static final Logger logger = LoggerFactory.getLogger(GeneticOptimizerApp.class);

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
     * The {@link #partialExecutions} grouped by their containing {@link ExecutionOperator}s.
     */
    private final List<List<PartialExecution>> partialExecutionGroups;

    /**
     * Maintains a {@link DynamicLoadProfileEstimator} for every {@link Configuration} key in the
     * {@link #partialExecutions}.
     */
    Map<String, DynamicLoadProfileEstimator> estimators;

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

        // Initialize platforms.
        Java.platform();
        Spark.platform();
        Sqlite3.platform();
        Postgres.platform();
        GraphChi.platform();

        // Load the ExecutionLog.
        double samplingFactor = this.configuration.getDoubleProperty("rheem.profiler.ga.sampling", 1d);
        double maxCardinalitySpread = this.configuration.getDoubleProperty("rheem.profiler.ga.max-cardinality-spread", 1d);
        double minCardinalityConfidence = this.configuration.getDoubleProperty("rheem.profiler.ga.min-cardinality-confidence", 1d);
        long minExecutionTime = this.configuration.getLongProperty("rheem.profiler.ga.min-exec-time", 1);
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            this.partialExecutions = executionLog.stream().collect(Collectors.toList());

            int lastSize = this.partialExecutions.size();
            this.partialExecutions.removeIf(partialExecution -> !this.checkEstimatorTemplates(partialExecution));
            int newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions with no template-based estimators.\n", lastSize - newSize);
            lastSize = newSize;

            this.partialExecutions.removeIf(partialExecution -> !this.checkSpread(partialExecution, maxCardinalitySpread));
            newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions with a too large cardinality spread (> %.2f).\n", lastSize - newSize, minCardinalityConfidence);
            lastSize = newSize;

            this.partialExecutions.removeIf(partialExecution -> !this.checkNonEmptyCardinalities(partialExecution));
            newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions with zero cardinalities.\n", lastSize - newSize);
            lastSize = newSize;

            this.partialExecutions.removeIf(partialExecution -> !this.checkConfidence(partialExecution, minCardinalityConfidence));
            newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions with a too low cardinality confidence (< %.2f).\n", lastSize - newSize, minCardinalityConfidence);
            lastSize = newSize;

            this.partialExecutions.removeIf(partialExecution -> partialExecution.getMeasuredExecutionTime() < minExecutionTime);
            newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions with a too short runtime (< %,d ms).\n", lastSize - newSize, minExecutionTime);
            lastSize = newSize;

            this.partialExecutions.removeIf(partialExecution -> new Random().nextDouble() > samplingFactor);
            newSize = this.partialExecutions.size();
            System.out.printf("Removed %d executions due to sampling.\n", lastSize - newSize);
        } catch (Exception e) {
            throw new RheemException("Could not evaluate execution log.", e);
        }

        // Group the PartialExecutions.
        this.partialExecutionGroups = this.groupPartialExecutions(this.partialExecutions).entrySet().stream()
                .sorted(Comparator.comparingInt(e -> e.getKey().size()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());

        // Apply binning if requested.
        double binningStretch = this.configuration.getDoubleProperty("rheem.profiler.ga.binning", 1.1d);
        if (binningStretch > 1d) {
            System.out.print("Applying binning... ");
            int numOriginalPartialExecutions = this.partialExecutions.size();
            this.partialExecutions.clear();
            for (List<PartialExecution> group : this.partialExecutionGroups) {
                final Collection<PartialExecution> reducedGroup = this.binByExecutionTime(group, binningStretch);
                group.retainAll(reducedGroup);
                this.partialExecutions.addAll(reducedGroup);
            }
            System.out.printf(
                    "reduced the number of partial executions from %d to %d.\n",
                    numOriginalPartialExecutions, this.partialExecutions.size()
            );
        }

        // Initialize the optimization space with its LoadProfileEstimators and associated Variables.
        this.optimizationSpace = new OptimizationSpace();
        this.estimators = new HashMap<>();
        this.platformOverheads = new HashMap<>();

        for (PartialExecution partialExecution : this.partialExecutions) {
            // Instrument the partial executions.
            for (AtomicExecutionGroup executionGroup : partialExecution.getAtomicExecutionGroups()) {
                for (AtomicExecution atomicExecution : executionGroup.getAtomicExecutions()) {
                    this.instrument(atomicExecution);
                }
            }

            for (Platform platform : partialExecution.getInitializedPlatforms()) {
                this.platformOverheads.computeIfAbsent(
                        platform,
                        key -> this.optimizationSpace.getOrCreateVariable(key.getClass().getCanonicalName() + "->overhead")
                );
            }
        }

        System.out.printf(
                "Loaded %d execution records with %d template-based estimators types and %d platform overheads.\n",
                this.partialExecutions.size(), estimators.keySet().size(), this.platformOverheads.size()
        );
    }

    /**
     * Check if all {@link CardinalityEstimate}s for the {@link PartialExecution} are sufficiently confident.
     *
     * @param partialExecution         whose {@link CardinalityEstimate}s should be checked
     * @param minCardinalityConfidence the minimum confidence
     * @return whether the {@link CardinalityEstimate}s are sufficiently confident
     */
    private boolean checkConfidence(PartialExecution partialExecution, double minCardinalityConfidence) {
        return partialExecution.getAtomicExecutionGroups().stream().allMatch(
                executionGroup -> {
                    final EstimationContext estimationContext = executionGroup.getEstimationContext();
                    for (CardinalityEstimate cardinality : estimationContext.getInputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getCorrectnessProbability() < minCardinalityConfidence) return false;
                    }
                    for (CardinalityEstimate cardinality : estimationContext.getOutputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getCorrectnessProbability() < minCardinalityConfidence) return false;
                    }
                    return true;
                }
        );
    }

    /**
     * Check if all {@link CardinalityEstimate}s for the {@link PartialExecution} are greater than zero.
     *
     * @param partialExecution whose {@link CardinalityEstimate}s should be checked
     * @return whether the {@link CardinalityEstimate}s are not equal to zero
     */
    private boolean checkNonEmptyCardinalities(PartialExecution partialExecution) {
        return partialExecution.getAtomicExecutionGroups().stream().allMatch(
                executionGroup -> {
                    final EstimationContext estimationContext = executionGroup.getEstimationContext();
                    for (CardinalityEstimate cardinality : estimationContext.getInputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getUpperEstimate() == 0) {
                            return false;
                        }
                    }
                    for (CardinalityEstimate cardinality : estimationContext.getOutputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getUpperEstimate() == 0) {
                            return false;
                        }
                    }
                    return true;
                }
        );
    }

    /**
     * Check if this {@link PartialExecution} contains any template-based {@link LoadProfileEstimator}s.
     *
     * @param partialExecution that should be checked
     * @return whether the {@code partialExecution} contains at least one template
     */
    private boolean checkEstimatorTemplates(PartialExecution partialExecution) {
        return !this.getLoadProfileEstimatorTemplateKeys(partialExecution).isEmpty();
    }

    /**
     * Check if all {@link CardinalityEstimate}s for the {@link PartialExecution} are sufficiently narrow.
     *
     * @param partialExecution     whose {@link CardinalityEstimate}s should be checked
     * @param maxCardinalitySpread the maximum spread of the {@link CardinalityEstimate}s
     * @return whether the {@link CardinalityEstimate}s are sufficiently narrow
     */
    private boolean checkSpread(PartialExecution partialExecution, double maxCardinalitySpread) {
        return partialExecution.getAtomicExecutionGroups().stream().allMatch(
                executionGroup -> {
                    final EstimationContext estimationContext = executionGroup.getEstimationContext();
                    for (CardinalityEstimate cardinality : estimationContext.getInputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getLowerEstimate() * maxCardinalitySpread < cardinality.getUpperEstimate()) {
                            return false;
                        }
                    }
                    for (CardinalityEstimate cardinality : estimationContext.getOutputCardinalities()) {
                        if (cardinality == null) continue;
                        if (cardinality.getLowerEstimate() * maxCardinalitySpread < cardinality.getUpperEstimate()) {
                            return false;
                        }
                    }
                    return true;
                }
        );
    }

    /**
     * Wrap the {@link LoadProfileEstimator}s of the given {@link AtomicExecution} with {@link DynamicLoadEstimator}s
     * where possible.
     *
     * @param atomicExecution that should be instrumented
     */
    private void instrument(AtomicExecution atomicExecution) {
        final DynamicLoadProfileEstimator dynamicLoadProfileEstimator = DynamicLoadProfileEstimators.createEstimatorFor(
                atomicExecution.getLoadProfileEstimator(),
                this.configuration,
                this.optimizationSpace
        );
        atomicExecution.setLoadProfileEstimator(dynamicLoadProfileEstimator);

        // Keep track of the estimators.
        Queue<LoadProfileEstimator> instrumentedEstimators = new LinkedList<>();
        instrumentedEstimators.add(dynamicLoadProfileEstimator);
        while (!instrumentedEstimators.isEmpty()) {
            final LoadProfileEstimator estimator = instrumentedEstimators.poll();
            if (estimator instanceof DynamicLoadProfileEstimator && estimator.getConfigurationKey() != null) {
                this.estimators.put(estimator.getConfigurationKey(), (DynamicLoadProfileEstimator) estimator);
            }
            instrumentedEstimators.addAll(estimator.getNestedEstimators());
        }
    }


    public void run() {
        if (this.optimizationSpace.getNumDimensions() == 0) {
            System.out.println("There is nothing to optimize - all estimators are specified in the configuration.");
            System.exit(0);
        }

        // Initialize form the configuration.
        long timeLimit = this.configuration.getLongProperty("rheem.profiler.ga.timelimit.ms", -1);
        long stopMillis = timeLimit > 0 ? System.currentTimeMillis() + timeLimit : -1L;
        int maxGen = (int) this.configuration.getLongProperty("rheem.profiler.ga.maxgenerations", 5000);
        int maxStableGen = (int) this.configuration.getLongProperty("rheem.profiler.ga.maxstablegenerations", 2000);
        double minFitness = this.configuration.getDoubleProperty("rheem.profiler.ga.minfitness", .0d);
        int superOptimizations = (int) this.configuration.getLongProperty("rheem.profiler.ga.superoptimizations", 3);
        boolean isBlocking = this.configuration.getBooleanProperty("rheem.profiler.ga.blocking", false);
        long maxPartialExecutionRemovals = this.configuration.getLongProperty("rheem.profiler.ga.noise-filter.max", 3);
        double partialExecutionRemovalThreshold = this.configuration.getDoubleProperty("rheem.profiler.ga.noise-filter.threshold", 2);

        // Create the root optimizer and an initial population.
        GeneticOptimizer generalOptimizer = this.createOptimizer(this.partialExecutions);
        List<Individual> population = generalOptimizer.createInitialPopulation();
        int generation = 0;

        // Optimize on blocks.
        if (isBlocking) {
            for (List<PartialExecution> group : this.partialExecutionGroups) {
                final PartialExecution representative = RheemCollections.getAny(group);
                final Set<String> templateKeys = this.getLoadProfileEstimatorTemplateKeys(representative);
                if (group.size() < 2) {
                    System.out.printf("Few measurement points for %s\n", templateKeys);
                }
                if (representative.getAtomicExecutionGroups().size() > 3) {
                    System.out.printf("Many subjects for %s\n", templateKeys);
                }

                long minExecTime = group.stream().mapToLong(PartialExecution::getMeasuredExecutionTime).min().getAsLong();
                long maxExecTime = group.stream().mapToLong(PartialExecution::getMeasuredExecutionTime).max().getAsLong();
                if (maxExecTime - minExecTime < 1000) {
                    System.out.printf("Narrow training data for %s\n", templateKeys);
                    continue;
                }

                final Tuple<Integer, List<Individual>> newGeneration = this.superOptimize(
                        superOptimizations, population, group, generation, maxGen, maxStableGen, minFitness, stopMillis
                );
                generation = newGeneration.getField0();
                population = newGeneration.getField1();

                final GeneticOptimizer tempOptimizer = this.createOptimizer(group);
                this.printResults(tempOptimizer, population.get(0));
            }
        }

        while (true) {
            // Optimize on the complete training data.
            final Tuple<Integer, List<Individual>> newGeneration = this.optimize(
                    population, generalOptimizer, generation, maxGen, maxStableGen, minFitness, stopMillis
            );
            generation = newGeneration.getField0();
            population = newGeneration.getField1();
            Individual fittestIndividual = population.get(0);
            printResults(generalOptimizer, fittestIndividual);

            if (maxPartialExecutionRemovals > 0) {
                // Gather the PartialExecutions that are not well explained by the learned model.
                List<Tuple<PartialExecution, Double>> partialExecutionDeviations = new ArrayList<>();
                for (PartialExecution partialExecution : partialExecutions) {
                    final double timeEstimate = fittestIndividual.estimateTime(
                            partialExecution, this.platformOverheads, this.configuration
                    );
                    double deviation = (Math.max(timeEstimate, partialExecution.getMeasuredExecutionTime()) + 500) /
                            (Math.min(timeEstimate, partialExecution.getMeasuredExecutionTime()) + 500);
                    if (deviation > partialExecutionRemovalThreshold) {
                        partialExecutionDeviations.add(new Tuple<>(partialExecution, deviation));
                    }
                }

                // Check if we actually have a good model.
                if (partialExecutionDeviations.isEmpty()) {
                    System.out.printf("All %d executions are explained well by the current model.\n", this.partialExecutions.size());
                    break;
                }

                // Check if we ran out of time.
                if (stopMillis > 0 && System.currentTimeMillis() >= stopMillis) break;

                // Remove the worst PartialExecutions.
                System.out.printf("The current model is not explaining well %d of %d measured executions.\n",
                        partialExecutionDeviations.size(),
                        this.partialExecutions.size()
                );
                partialExecutionDeviations.sort((ped1, ped2) -> ped2.getField1().compareTo(ped1.getField1()));
                long numRemovables = maxPartialExecutionRemovals;
                for (Tuple<PartialExecution, Double> partialExecutionDeviation : partialExecutionDeviations) {
                    if (numRemovables-- <= 0) break;
                    final PartialExecution partialExecution = partialExecutionDeviation.getField0();
                    final double deviation = partialExecutionDeviation.getField1();
                    final double timeEstimate = fittestIndividual.estimateTime(
                            partialExecution, this.platformOverheads, this.configuration
                    );
                    System.out.printf("Removing %s... (estimated %s, deviation %,.2f)\n",
                            format(partialExecution), Formats.formatDuration(Math.round(timeEstimate)), deviation
                    );
                    this.partialExecutions.remove(partialExecution);
                }
            } else {
                break;
            }
        }

        String outputFile = this.configuration.getStringProperty("rheem.profiler.ga.output-file", null);
        if (outputFile != null) {
            Individual fittestIndividual = population.get(0);
            try (PrintStream printStream = new PrintStream(new FileOutputStream(outputFile))) {
                this.printLearnedConfiguration(generalOptimizer, fittestIndividual, printStream);
            } catch (FileNotFoundException e) {
                logger.error("Could not save learned configuration to output file.", e);
            }
        }

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
            final double timeEstimate = individual.estimateTime(partialExecution, this.platformOverheads, this.configuration);
            System.out.printf("Actual %13s | Estimated: %72s | %3d execution groups | %s\n",
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    Formats.formatDuration(Math.round(timeEstimate)),
                    partialExecution.getAtomicExecutionGroups().size(),
                    Stream.concat(
                            partialExecution.getAtomicExecutionGroups().stream().map(AtomicExecutionGroup::toString),
                            partialExecution.getInitializedPlatforms().stream().map(Platform::getName)
                    ).collect(Collectors.toList())
            );
        }

        System.out.println();
        System.out.println("Configuration file");
        System.out.println("==================");
        this.printLearnedConfiguration(optimizer, individual, System.out);
    }

    private void printLearnedConfiguration(GeneticOptimizer optimizer, Individual individual, PrintStream out) {
        final Bitmask genes = optimizer.getActivatedGenes();
        Set<Variable> optimizedVariables = new HashSet<>(genes.cardinality());
        for (int gene = genes.nextSetBit(0); gene != -1; gene = genes.nextSetBit(gene + 1)) {
            optimizedVariables.add(this.optimizationSpace.getVariable(gene));
        }
        for (Map.Entry<Platform, Variable> entry : this.platformOverheads.entrySet()) {
            final Platform platform = entry.getKey();
            final Variable overhead = entry.getValue();
            if (!optimizedVariables.contains(overhead)) continue;

            out.printf("rheem.%s.init.ms = %d\n",
                    platform.getConfigurationName(),
                    Math.round(overhead.getValue(individual))
            );
        }
        for (LoadProfileEstimator estimator : estimators.values()) {
            if (estimator instanceof DynamicLoadProfileEstimator) {
                final DynamicLoadProfileEstimator dynamicLoadProfileEstimator = (DynamicLoadProfileEstimator) estimator;
                if (!optimizedVariables.containsAll(dynamicLoadProfileEstimator.getEmployedVariables())) continue;
                out.println(dynamicLoadProfileEstimator.toJsonConfig(individual));
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
        return new GeneticOptimizer(
                this.optimizationSpace,
                partialExecutions,
                this.estimators,
                this.platformOverheads,
                this.configuration
        );
    }

    private Tuple<Integer, List<Individual>> superOptimize(
            int numTribes,
            List<Individual> individuals,
            Collection<PartialExecution> partialExecutions,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness,
            long stopMillis) {

        int individualsPerTribe = (individuals.size() + numTribes - 1) / numTribes;
        List<Individual> superpopulation = new ArrayList<>(individuals.size() * numTribes);
        int maxGeneration = 0;
        for (int i = 0; i < numTribes; i++) {
            final Tuple<Integer, List<Individual>> population = this.optimize(
                    individuals, partialExecutions, currentGeneration, maxGenerations, maxStableGenerations, minFitness, stopMillis
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
            double minFitness,
            long stopMillis) {
        GeneticOptimizer optimizer = this.createOptimizer(partialExecutions);
        return this.optimize(individuals, optimizer, currentGeneration, maxGenerations, maxStableGenerations, minFitness, stopMillis);
    }

    private Tuple<Integer, List<Individual>> optimize(
            List<Individual> individuals,
            GeneticOptimizer optimizer,
            int currentGeneration,
            int maxGenerations,
            int maxStableGenerations,
            double minFitness,
            long stopMillis) {

        if (optimizer.getActivatedGenes().isEmpty()) {
            System.out.println("There is an optimization task without optimizable genes. It will be skipped");
            return new Tuple<>(currentGeneration, individuals);
        }

        int updateFrequency = (int) this.configuration.getLongProperty("rheem.profiler.ga.intermediateupdate", 10000);
        System.out.printf("Optimizing %d variables on %d partial executions (e.g., %s).\n",
                optimizer.getActivatedGenes().cardinality(),
                optimizer.getData().size(),
                RheemCollections.getAny(optimizer.getData()).getAtomicExecutionGroups()
        );

        optimizer.updateFitness(individuals);
        double checkpointedFitness = Double.NEGATIVE_INFINITY;
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

            if (updateFrequency > 0 && i > 0 && i % updateFrequency == 0) {
                System.out.println("Intermediate update:");
                this.printResults(optimizer, individuals.get(0));
            }

            // Check if the time limit has passed.
            if (stopMillis > 0 && stopMillis <= System.currentTimeMillis()) break;

            // Check whether we seem to be stuck in a (local) optimum.
            if (i % maxStableGenerations == 0) {
                final double currentFitness = individuals.get(0).getFitness();
                if (!(currentFitness >= checkpointedFitness + 0.001) && currentFitness >= minFitness && i > 0) {
                    break;
                } else {
                    checkpointedFitness = currentFitness;
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
     * Group {@link PartialExecution}s by their comprised {@link LoadProfileEstimator}s template.
     *
     * @param partialExecutions the {@link PartialExecution}s
     * @return the grouping of the {@link #partialExecutions}
     */
    private Map<Set<String>, List<PartialExecution>> groupPartialExecutions(Collection<PartialExecution> partialExecutions) {
        Map<Set<String>, List<PartialExecution>> groups = new HashMap<>();
        for (PartialExecution partialExecution : partialExecutions) {

            // Determine the ExecutionOperator classes in the partialExecution.
            final Set<String> templateKeys = this.getLoadProfileEstimatorTemplateKeys(partialExecution);

            // Index the partialExecution.
            groups.computeIfAbsent(templateKeys, key -> new LinkedList<>()).add(partialExecution);
        }

        return groups;
    }

    /**
     * Extract the {@link LoadProfileEstimator} template keys in the given {@link PartialExecution}.
     *
     * @param partialExecution the {@link PartialExecution}
     * @return the {@link ExecutionOperator} {@link Class}es
     */
    private Set<String> getLoadProfileEstimatorTemplateKeys(PartialExecution partialExecution) {
        return partialExecution.getAtomicExecutionGroups().stream()
                .flatMap(group -> group.getAtomicExecutions().stream())
                .flatMap(execution -> execution.getLoadProfileEstimator().getTemplateKeys().stream())
                .collect(Collectors.toSet());
    }

    /**
     * Bin given {@link PartialExecution}s by their execution time and retain one representative per bin.
     *
     * @param partialExecutions the {@link PartialExecution}s
     * @param densityFactor     the stretch of each bin
     * @return the binned {@link PartialExecution}s
     */
    private Collection<PartialExecution> binByExecutionTime(Collection<PartialExecution> partialExecutions, double densityFactor) {
        Map<Integer, PartialExecution> resultBins = new HashMap<>();
        for (PartialExecution partialExecution : partialExecutions) {
            int key = (int) Math.round(Math.log1p(partialExecution.getMeasuredExecutionTime()) / Math.log(densityFactor));
            resultBins.put(key, partialExecution);
        }
        return resultBins.values();
    }

    private static String format(PartialExecution partialExecution) {
        return String.format("[%d atomic execution groups in %s: %s, %s]",
                partialExecution.getAtomicExecutionGroups().size(),
                Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                partialExecution.getAtomicExecutionGroups(),
                partialExecution.getInitializedPlatforms()
        );
    }

    public static void main(String[] args) {
        Configuration configuration = args.length == 0 ? new Configuration() : new Configuration(args[0]);
        if (args.length >= 2) {
            configuration.setProperty("rheem.core.log.executions", args[1]);
        }
        new GeneticOptimizerApp(configuration).run();
    }
}
