package org.qcri.rheem.profiler.log;

import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Utility to create {@link DynamicLoadProfileEstimator}s.
 */
public class DynamicLoadProfileEstimators {

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLinearEstimator(ExecutionOperator operator,
                                                                    OptimizationSpace optimizationSpace) {
        // Create variables.
        Variable[] inVars = new Variable[operator.getNumInputs()];
        for (int i = 0; i < inVars.length; i++) {
            inVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getClass().getSimpleName() + "->" + operator.getInput(i).getName()
            );
        }
        Variable[] outVars = new Variable[operator.getNumOutputs()];
        for (int i = 0; i < outVars.length; i++) {
            outVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getClass().getSimpleName() + "->" + operator.getOutput(i).getName()
            );
        }
        Variable offsetVar = optimizationSpace.getOrCreateVariable(
                operator.getClass().getSimpleName() + "->offset"
        );

        // Create the estimation function.
        final DynamicLoadEstimator.SinglePointEstimator singlePointEstimator = (individual, in, out) -> {
            double accu = offsetVar.getValue(individual);
            for (int inputIndex = 0; inputIndex < inVars.length; inputIndex++) {
                accu += inVars[inputIndex].getValue(individual) * in[inputIndex];
            }
            for (int outputIndex = 0; outputIndex < outVars.length; outputIndex++) {
                accu += outVars[outputIndex].getValue(individual) * out[outputIndex];
            }
            return accu;
        };

        // Create the JUEL template.
        StringBuilder sb = new StringBuilder().append("${");
        for (int i = 0; i < inVars.length; i++) {
            sb.append("%s*in").append(i).append(" + ");
        }
        for (int i = 0; i < outVars.length; i++) {
            sb.append("%s*out").append(i).append(" + ");
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

}
