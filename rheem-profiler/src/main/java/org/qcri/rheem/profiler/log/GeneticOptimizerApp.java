package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.PartialExecution;
import org.qcri.rheem.core.profiling.ExecutionLog;
import org.qcri.rheem.core.util.Formats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
        for (PartialExecution partialExecution : this.partialExecutions) {
            for (PartialExecution.OperatorExecution execution : partialExecution.getOperatorExecutions()) {
                estimators.computeIfAbsent(
                        execution.getOperator().getClass(),
                        key -> this.createEstimator(execution.getOperator())
                );
            }
        }
        System.out.printf("Found %d execution operator types.\n", estimators.size());

        // Create a random individual and calculate its fitness.
        Random random = new Random();
        Individual bestIndividual = this.optimizationSpace.createRandomIndividual(random);
        double bestFitness = bestIndividual.calculateFitness(this.partialExecutions, estimators, this.configuration);
        int generation = 0;
        while (generation++ < 20000) {
            final Individual individual = bestIndividual.mutate(random, this.optimizationSpace, 0.2d, 0.05d);
            final double fitness = individual.calculateFitness(this.partialExecutions, estimators, this.configuration);
            if (fitness > bestFitness) {
                System.out.println("Fitness of best individual: " + fitness);
                bestFitness = fitness;
                bestIndividual = individual;
            }
        }

        // Print the variable values.
        for (Variable variable : this.optimizationSpace.getVariables()) {
            System.out.printf("%s -> %.2f\n", variable.getId(), variable.getValue(bestIndividual));
        }

        for (PartialExecution partialExecution : this.partialExecutions) {
            final TimeEstimate timeEstimate = bestIndividual.estimateTime(partialExecution, estimators, this.configuration);
            System.out.printf("Estimated: %s, actual %s (%d operators)\n",
                    timeEstimate,
                    Formats.formatDuration(partialExecution.getMeasuredExecutionTime()),
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
        switch (10 *operator.getNumInputs() + operator.getNumOutputs()) {
            case 10:
                // unary sink
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                );
            case 1:
                // unary source
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> out[0] * outVars[0].getValue(ind)
                );
            case 11:
                // one-to-one
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind) + out[0] * outVars[0].getValue(ind)
                );
            case 21:
                // two-to-one
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind) + out[0] * outVars[0].getValue(ind)
                );
            case 32:
                // do-while loop
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                                + in[1] * inVars[1].getValue(ind)
                                + in[2] * inVars[2].getValue(ind)
                                + out[0] * outVars[0].getValue(ind)
                                + out[1] * outVars[1].getValue(ind)
                );
            case 22:
                // repeat loop
                return new DynamicLoadProfileEstimator(
                        (ind, in, out) -> in[0] * inVars[0].getValue(ind)
                                + in[1] * inVars[1].getValue(ind)
                                + out[0] * outVars[0].getValue(ind)
                                + out[1] * outVars[1].getValue(ind)
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
                                + out[2] * outVars[2].getValue(ind)
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
