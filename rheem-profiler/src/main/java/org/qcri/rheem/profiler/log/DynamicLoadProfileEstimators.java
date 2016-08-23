package org.qcri.rheem.profiler.log;

import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.operators.JavaCollectOperator;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;
import org.qcri.rheem.spark.operators.SparkBroadcastOperator;
import org.qcri.rheem.spark.operators.SparkCacheOperator;
import org.qcri.rheem.spark.operators.SparkCollectOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Utility to create {@link DynamicLoadProfileEstimator}s.
 */
public class DynamicLoadProfileEstimators {

    /**
     * Let this class try to find a suitable {@link DynamicLoadProfileEstimator} for the given {@link ExecutionOperator}.
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createSuitableEstimator(ExecutionOperator operator,
                                                                      OptimizationSpace optimizationSpace) {

        // Special treatment of such unary operators that have a static number of output data quanta.
        boolean isMapLike = (operator.getNumInputs() == 1 && operator.getNumOutputs() == 1) &&
                (operator instanceof MapOperator || operator instanceof SortOperator
                        || operator instanceof SqlToStreamOperator || operator instanceof JavaCollectOperator
                        || operator instanceof SparkCacheOperator || operator instanceof SparkBroadcastOperator
                        || operator instanceof SparkCollectOperator || operator instanceof ZipWithIdOperator);
        boolean isGlobalReduction = (operator.getNumInputs() == 1 && operator.getNumOutputs() == 1) &&
                (operator instanceof CountOperator || operator instanceof GlobalReduceOperator
                        || operator instanceof GlobalMaterializedGroupOperator);
        if (isMapLike || isGlobalReduction) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], optimizationSpace);
        }

        // Special treatment of such binay operator that have a static number of output data quanta.
        if (operator instanceof UnionAllOperator || operator instanceof CartesianOperator) {
            return createLinearEstimator(operator, new int[0], new int[]{0}, optimizationSpace);
        }

        // Special treatment of loop head operators.
        if (operator.isLoopHead()) {
            return createLoopEstimator(operator, optimizationSpace);
        }

        return createLinearEstimator(operator, optimizationSpace);
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLinearEstimator(ExecutionOperator operator,
                                                                    OptimizationSpace optimizationSpace) {
        return createLinearEstimator(
                operator,
                RheemArrays.range(operator.getNumInputs()),
                RheemArrays.range(operator.getNumOutputs()),
                optimizationSpace
        );
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param inputIndices      indices of {@link InputSlot}s for which {@link Variable}s should be created
     * @param outputIndices     indices of {@link OutputSlot}s for which {@link Variable}s should be created
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLinearEstimator(ExecutionOperator operator,
                                                                    int[] inputIndices,
                                                                    int[] outputIndices,
                                                                    OptimizationSpace optimizationSpace) {
        // Create variables.
        Variable[] inVars = new Variable[inputIndices.length];
        for (int i = 0; i < inputIndices.length; i++) {
            int index = inputIndices[i];
            inVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getInput(index).getName()
            );
        }
        Variable[] outVars = new Variable[outputIndices.length];
        for (int i = 0; i < outputIndices.length; i++) {
            int index = outputIndices[i];
            outVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getOutput(index).getName()
            );
        }
        Variable offsetVar = optimizationSpace.getOrCreateVariable(
                operator.getLoadProfileEstimatorConfigurationKey() + "->offset"
        );

        // Create the estimation function.
        final DynamicLoadEstimator.SinglePointEstimator singlePointEstimator = (individual, in, out) -> {
            double accu = offsetVar.getValue(individual);
            for (int i = 0; i < inputIndices.length; i++) {
                accu += inVars[i].getValue(individual) * in[inputIndices[i]];
            }
            for (int i = 0; i < outputIndices.length; i++) {
                accu += outVars[i].getValue(individual) * out[outputIndices[i]];
            }
            return accu;
        };

        // Create the JUEL template.
        StringBuilder sb = new StringBuilder().append("${");
        for (int i = 0; i < inputIndices.length; i++) {
            sb.append("%s*in").append(inputIndices[i]).append(" + ");
        }
        for (int i = 0; i < outputIndices.length; i++) {
            sb.append("%s*out").append(outputIndices[i]).append(" + ");
        }
        sb.append("%s}");
        String juelTemplate = sb.toString();

        // Gather the employed variables.
        Collection<Variable> employedVariables = new LinkedList<>();
        employedVariables.addAll(Arrays.asList(inVars));
        employedVariables.addAll(Arrays.asList(outVars));
        employedVariables.add(offsetVar);

        // Assemble the estimator.
        return new DynamicLoadProfileEstimator(
                operator.getLoadProfileEstimatorConfigurationKey(),
                operator.getNumInputs(),
                operator.getNumOutputs(),
                new DynamicLoadEstimator(singlePointEstimator, juelTemplate, employedVariables)
        );
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is specifically adapted to {@link LoopHeadOperator}s.
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLoopEstimator(ExecutionOperator operator,
                                                                  OptimizationSpace optimizationSpace) {
        assert operator.isLoopHead();

        int[] mainInputIndices;
        int[] convergenceInputIndices;

        if (operator instanceof LoopOperator) {
            mainInputIndices = new int[]{LoopOperator.INITIAL_INPUT_INDEX, LoopOperator.ITERATION_INPUT_INDEX};
            convergenceInputIndices = new int[]{LoopOperator.INITIAL_CONVERGENCE_INPUT_INDEX, LoopOperator.ITERATION_CONVERGENCE_INPUT_INDEX};
        } else if (operator instanceof DoWhileOperator) {
            mainInputIndices = new int[]{DoWhileOperator.INITIAL_INPUT_INDEX, DoWhileOperator.ITERATION_INPUT_INDEX};
            convergenceInputIndices = new int[]{DoWhileOperator.CONVERGENCE_INPUT_INDEX};
        } else if (operator instanceof RepeatOperator) {
            mainInputIndices = new int[]{RepeatOperator.INITIAL_INPUT_INDEX, RepeatOperator.ITERATION_INPUT_INDEX};
            convergenceInputIndices = new int[0];
        } else {
            throw new IllegalArgumentException("Unsupported loop operator: " + operator);
        }

        // Create variables.
        Variable mainVar = optimizationSpace.getOrCreateVariable(
                operator.getLoadProfileEstimatorConfigurationKey() + "->main"
        );
        Variable convergenceVar = convergenceInputIndices.length > 0 ?
                optimizationSpace.getOrCreateVariable(
                        operator.getLoadProfileEstimatorConfigurationKey() + "->convergence"
                ) :
                null;
        Variable offsetVar = optimizationSpace.getOrCreateVariable(
                operator.getLoadProfileEstimatorConfigurationKey() + "->offset"
        );

        // Create the estimation function.
        final DynamicLoadEstimator.SinglePointEstimator singlePointEstimator = (individual, in, out) -> {
            double accu = offsetVar.getValue(individual);
            for (int inputIndex : mainInputIndices) {
                accu += in[inputIndex] * mainVar.getValue(individual);
            }
            for (int inputIndex : convergenceInputIndices) {
                accu += in[inputIndex] * convergenceVar.getValue(individual);
            }
            return accu;
        };

        // Create the JUEL template.
        StringBuilder sb = new StringBuilder().append("${%s*(");
        String separator = "";
        for (int i = 0; i < mainInputIndices.length; i++) {
            sb.append(separator).append("in").append(mainInputIndices[i]);
            separator = " + ";
        }
        sb.append(") + ");
        if (convergenceVar != null) {
            sb.append("%s*(");
            separator = "";
            for (int i = 0; i < convergenceInputIndices.length; i++) {
                sb.append(separator).append("in").append(convergenceInputIndices[i]);
                separator = " + ";
            }
            sb.append(") + ");
        }
        sb.append("%s}");
        String juelTemplate = sb.toString();

        // Gather the employed variables.
        Collection<Variable> employedVariables = new LinkedList<>();
        employedVariables.add(mainVar);
        if (convergenceVar != null) employedVariables.add(convergenceVar);
        employedVariables.add(offsetVar);

        // Assemble the estimator.
        return new DynamicLoadProfileEstimator(
                operator.getLoadProfileEstimatorConfigurationKey(),
                operator.getNumInputs(),
                operator.getNumOutputs(),
                new DynamicLoadEstimator(singlePointEstimator, juelTemplate, employedVariables)
        );
    }

}
