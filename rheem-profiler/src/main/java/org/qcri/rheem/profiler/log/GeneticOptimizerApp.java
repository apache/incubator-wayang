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

import java.util.*;
import java.util.stream.Collectors;

/**
 * This app tries to infer good {@link LoadProfileEstimator}s for {@link ExecutionOperator}s using data from an
 * {@link ExecutionLog}.
 */
public class GeneticOptimizerApp {

    private final Configuration configuration;

    private OptimizationSpace optimizationSpace;

    private List<PartialExecution> partialExecutions;

    public GeneticOptimizerApp(Configuration configuration) {
        this.configuration = configuration;
    }

    private void run() {
        // Load the execution log.
        try (ExecutionLog executionLog = ExecutionLog.open(configuration)) {
            this.partialExecutions = executionLog.stream().collect(Collectors.toList());
        } catch (Exception e) {
            throw new RheemException("Could not evaluate execution log.", e);
        }

        // Initialize the optimization space.
        this.optimizationSpace = new OptimizationSpace();

        // Gather operator types present in the execution log.
        Map<Class<? extends ExecutionOperator>, LoadProfileEstimator<Individual>> estimators = new HashMap<>();
        Map<Set<Class<? extends ExecutionOperator>>, List<PartialExecution>> partialExecutionClasses = new HashMap<>();
        for (PartialExecution partialExecution : this.partialExecutions) {

            // Index the PartialExecution by its ExecutionOperators.
            final Set<Class<? extends ExecutionOperator>> execOpClasses =
                    partialExecution.getOperatorExecutions().stream()
                            .map(PartialExecution.OperatorExecution::getOperator)
                            .map(ExecutionOperator::getClass)
                            .collect(Collectors.toSet());
            partialExecutionClasses
                    .computeIfAbsent(execOpClasses, key -> new LinkedList<>())
                    .add(partialExecution);

            // Initialize an LoadProfileEstimator for each of the ExecutionOperators.
            for (PartialExecution.OperatorExecution execution : partialExecution.getOperatorExecutions()) {
                estimators.computeIfAbsent(
                        execution.getOperator().getClass(),
                        key -> this.createEstimator(execution.getOperator())
                );
            }
        }
        System.out.printf(
                "Found %d execution operator types in %d partial executions of %d classes.\n",
                estimators.size(), this.partialExecutions.size(), partialExecutionClasses.size()
        );
        System.out.printf("Going to optimize %d variables.\n", this.optimizationSpace.getNumDimensions());

        List<List<PartialExecution>> samples;
//        samples = new ArrayList<>();
//        for (int i = 0; i < 10; i++) {
//            List<PartialExecution> partialExecutionSample = this.partialExecutions.stream()
//                    .filter(pe -> random.nextDouble() < 0.01)
//                    .collect(Collectors.toList());
//            GeneticOptimizer optimizer = new GeneticOptimizer(
//                    this.optimizationSpace, partialExecutionSample, estimators, this.configuration
//            );
//            optimizer.updateFitness(population);
//            for (int j = 0; j < 10000; j++) {
//                population = optimizer.evolve(population);
//            }
//            System.out.printf("Fittest individual of generation %,d: %,.4f\n", i, population.get(0).getFitness());
//        }
        samples = partialExecutionClasses.entrySet().stream()
                .sorted((e1, e2) -> Integer.compare(e1.getKey().size(), e2.getKey().size()))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());


        int overallGen = 0;
        GeneticOptimizer generalOptimizer = new GeneticOptimizer(
                this.optimizationSpace, this.partialExecutions, estimators, this.configuration
        );
        List<Individual> population = generalOptimizer.createInitialPopulation();
        for (List<PartialExecution> partialExecutionSample : samples) {
            System.out.printf("Processing sample of %d partial executions (e.g., %s).\n",
                    partialExecutionSample.size(),
                    RheemCollections.getAny(partialExecutionSample).getOperatorExecutions());

            GeneticOptimizer optimizer = new GeneticOptimizer(
                    this.optimizationSpace, partialExecutionSample, estimators, this.configuration
            );
            optimizer.updateFitness(population);
            double checkpointFitness = Double.NEGATIVE_INFINITY;
            for (int j = 0; j < 1000; j++) {
                population = optimizer.evolve(population);
                if (j % 1000 == 0) {
                    System.out.printf("Fittest individual of generation: %,.4f\n", population.get(0).getFitness());
                }
                if (j % 1000 == 0) {
                    final double bestFitness = population.get(0).getFitness();
                    if (checkpointFitness >= bestFitness) {
                        break;
                    } else {
                        checkpointFitness = bestFitness;
                    }
                }
            }
            System.out.printf("Fittest individual of generation: %,.4f\n", population.get(0).getFitness());
        }

        generalOptimizer.updateFitness(population);
        System.out.printf("Fittest individual: %,.4f\n", population.get(0).getFitness());
        for (int i = 1; i <= 100; i++) {
            population = generalOptimizer.evolve(population);
        }
        System.out.printf("Fittest individual: %,.4f\n", population.get(0).getFitness());
        final Individual fittestIndividual = population.get(0);


        // Print the variable values.
        for (Variable variable : this.optimizationSpace.getVariables()) {
            System.out.printf("%s -> %.2f\n", variable.getId(), variable.getValue(fittestIndividual));
        }

        this.partialExecutions.sort((e1, e2) -> Long.compare(e2.getMeasuredExecutionTime(), e1.getMeasuredExecutionTime()));
        for (PartialExecution partialExecution : this.partialExecutions) {
            final TimeEstimate timeEstimate = fittestIndividual.estimateTime(partialExecution, estimators, this.configuration);
            System.out.printf("Actual %s; estimated: %s (%d operators)\n",
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
                    timeEstimate,
                    partialExecution.getOperatorExecutions().size());
        }
    }

    private LoadProfileEstimator<Individual> createEstimator(ExecutionOperator operator) {
        Variable[] inVars = new Variable[operator.getNumInputs()];
        for (int i = 0; i < inVars.length; i++) {
            inVars[i] = this.optimizationSpace.getOrCreateVariable(
                    operator.getClass().getSimpleName() + "->" + operator.getInput(i).getName()
            );
        }
        Variable[] outVars = new Variable[operator.getNumOutputs()];
        for (int i = 0; i < outVars.length; i++) {
            outVars[i] = this.optimizationSpace.getOrCreateVariable(
                    operator.getClass().getSimpleName() + "->" + operator.getOutput(i).getName()
            );
        }
        Collection<Variable> employedVariables = new LinkedList<>();
        employedVariables.addAll(Arrays.asList(inVars));
        employedVariables.addAll(Arrays.asList(outVars));
        switch (10 * operator.getNumInputs() + operator.getNumOutputs()) {
            case 10:
                // unary sink
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind),
                        employedVariables
                );
            case 1:
                // unary source
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> out[0] * outVars[0].getValue(ind),
                        employedVariables
                );
            case 11:
                // one-to-one
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind) + out[0] * outVars[0].getValue(ind),
                        employedVariables
                );
            case 21:
                // two-to-one
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind) + out[0] * outVars[0].getValue(ind),
                        employedVariables
                );
            case 32:
                // do-while loop
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                                + in[1] * inVars[1].getValue(ind)
                                + in[2] * inVars[2].getValue(ind)
                                + out[0] * outVars[0].getValue(ind)
                                + out[1] * outVars[1].getValue(ind),
                        employedVariables
                );
            case 22:
                // repeat loop
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                                + in[1] * inVars[1].getValue(ind)
                                + out[0] * outVars[0].getValue(ind)
                                + out[1] * outVars[1].getValue(ind),
                        employedVariables
                );
            case 43:
                // loop
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                                + in[1] * inVars[1].getValue(ind)
                                + in[2] * inVars[2].getValue(ind)
                                + in[3] * inVars[3].getValue(ind)
                                + out[0] * outVars[0].getValue(ind)
                                + out[1] * outVars[1].getValue(ind)
                                + out[2] * outVars[2].getValue(ind),
                        employedVariables
                );
            default:
                throw new RuntimeException("Cannot create estimator for " + operator);
        }
    }

    public static void main(String[] args) {
        Configuration configuration = new Configuration();

        new GeneticOptimizerApp(configuration).run();
    }
}
