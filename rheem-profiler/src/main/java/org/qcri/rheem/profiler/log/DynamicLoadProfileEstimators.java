package org.qcri.rheem.profiler.log;

import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.costs.*;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopHeadOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.graphchi.operators.GraphChiPageRankOperator;
import org.qcri.rheem.java.operators.*;
import org.qcri.rheem.java.operators.graph.JavaPageRankOperator;
import org.qcri.rheem.jdbc.operators.JdbcFilterOperator;
import org.qcri.rheem.jdbc.operators.JdbcProjectionOperator;
import org.qcri.rheem.jdbc.operators.JdbcTableSource;
import org.qcri.rheem.jdbc.operators.SqlToStreamOperator;
import org.qcri.rheem.spark.operators.*;
import org.qcri.rheem.spark.operators.graph.SparkPageRankOperator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
     * @param configuration     provides configuration values
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createSuitableEstimator(ExecutionOperator operator,
                                                                      OptimizationSpace optimizationSpace,
                                                                      Configuration configuration) {

        // JavaExecutionOperators.

        // Map-like operators.
        if (operator instanceof JavaMapOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaFilterOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaFlatMapOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaMapPartitionsOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaRandomSampleOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaReservoirSampleOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);

            // Reduce-like operators
        } else if (operator instanceof JavaReduceByOperator) {
            return createQuadraticEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaMaterializedGroupByOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaDistinctOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof JavaSortOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaGlobalReduceOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaGlobalMaterializedGroupOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaCountOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);

            // Binary operators.
        } else if (operator instanceof JavaIntersectOperator) {
            return createLinearEstimator(operator, new int[]{0, 1}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaUnionAllOperator) {
            return createLinearEstimator(operator, new int[0], new int[0], true, optimizationSpace);
        } else if (operator instanceof JavaCartesianOperator) {
            return createLinearEstimator(operator, new int[0], new int[]{0}, false, optimizationSpace);
        } else if (operator instanceof JavaJoinOperator) {
            return createLinearEstimator(operator, new int[]{0, 1}, new int[]{0}, false, optimizationSpace);

            // Loop operators.
        } else if (operator instanceof JavaLoopOperator) {
            return createLoopEstimator(operator, optimizationSpace);
        } else if (operator instanceof JavaDoWhileOperator) {
            return createLoopEstimator(operator, optimizationSpace);
        } else if (operator instanceof JavaRepeatOperator) {
            return createLoopEstimator(operator, optimizationSpace);

            // Sources and sinks.
        } else if (operator instanceof JavaCollectionSource) {
            return createLinearEstimator(operator, new int[0], new int[0], true, optimizationSpace);
        } else if (operator instanceof JavaLocalCallbackSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof JavaTextFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof JavaTextFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof JavaObjectFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof JavaObjectFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof JavaTsvFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof JavaTsvFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);

            // Graph operators.
        } else if (operator instanceof JavaPageRankOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{0}, false, optimizationSpace);

            // Conversion operators.
        } else if (operator instanceof JavaCollectOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SqlToStreamOperator) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        }

        // SparkExecutionOperators.

        // Map-like operators.
        else if (operator instanceof SparkMapOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SparkFilterOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkFlatMapOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkMapPartitionsOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkBernoulliSampleOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkRandomPartitionSampleOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkShufflePartitionSampleOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof ZipWithIdOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);

            // Reduce-like operators
        } else if (operator instanceof SparkReduceByOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkMaterializedGroupByOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SparkDistinctOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, false, optimizationSpace);
        } else if (operator instanceof SparkSortOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SparkGlobalReduceOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkGlobalMaterializedGroupOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkCountOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);

            // Binary operators.
        } else if (operator instanceof SparkIntersectOperator) {
            return createLinearEstimator(operator, new int[]{0, 1}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SparkUnionAllOperator) {
            return createLinearEstimator(operator, new int[0], new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkCartesianOperator) {
            return createLinearEstimator(operator, new int[0], new int[]{0}, false, optimizationSpace);
        } else if (operator instanceof SparkJoinOperator) {
            return createLinearEstimator(operator, new int[]{0, 1}, new int[]{0}, false, optimizationSpace);

            // Loop operators.
        } else if (operator instanceof SparkLoopOperator) {
            return createLoopEstimator(operator, optimizationSpace);
        } else if (operator instanceof SparkDoWhileOperator) {
            return createLoopEstimator(operator, optimizationSpace);
        } else if (operator instanceof SparkRepeatOperator) {
            return createLoopEstimator(operator, optimizationSpace);

            // Sources and sinks.
        } else if (operator instanceof SparkCollectionSource) {
            return createLinearEstimator(operator, new int[0], new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkLocalCallbackSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], false, optimizationSpace);
        } else if (operator instanceof SparkTextFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof SparkTextFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkObjectFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof SparkObjectFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkTsvFileSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof SparkTsvFileSink) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);

            // Graph operators.
        } else if (operator instanceof SparkPageRankOperator) {
            return createQuadraticEstimator(operator, new int[]{0}, new int[]{0}, true, optimizationSpace);

            // Conversion operators.
        } else if (operator instanceof SparkCollectOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        } else if (operator instanceof SparkCacheOperator) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof SparkBroadcastOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{}, true, optimizationSpace);
        }

        // JdbcExecutionOperators.
        else if (operator instanceof JdbcTableSource) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, true, optimizationSpace);
        } else if (operator instanceof JdbcFilterOperator) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, false, optimizationSpace);
        } else if (operator instanceof JdbcProjectionOperator) {
            return createLinearEstimator(operator, new int[]{}, new int[]{0}, false, optimizationSpace);

            // GraphChiExecutionOperators.
        } else if (operator instanceof GraphChiPageRankOperator) {
            return createLinearEstimator(operator, new int[]{0}, new int[]{0}, true, optimizationSpace);
        }

        // Otherwise, use heuristics.
        System.out.printf("Creating load profile estimator for %s heuristically.\n", operator);

        return createSuitableEstimatorHeuristically(operator, optimizationSpace, configuration);
    }

    private static DynamicLoadProfileEstimator createSuitableEstimatorHeuristically(
            ExecutionOperator operator,
            OptimizationSpace optimizationSpace,
            Configuration configuration) {
        // First check if the configuration already provides an estimator.
        final String key = operator.getLoadProfileEstimatorConfigurationKey();
        final String juelSpec = configuration.getProperties().provideLocally(key);
        if (juelSpec != null) {
            return wrap(LoadProfileEstimators.createFromJuelSpecification(juelSpec));
        }

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
            return createLinearEstimator(operator, new int[]{0}, new int[0], true, optimizationSpace);
        }

        // Special treatment of such binay operator that have a static number of output data quanta.
        if (operator instanceof UnionAllOperator || operator instanceof CartesianOperator) {
            return createLinearEstimator(operator, new int[0], new int[]{0}, true, optimizationSpace);
        }

        // Special treatment of loop head operators.
        if (operator.isLoopHead()) {
            return createLoopEstimator(operator, optimizationSpace);
        }

        return createLinearEstimator(operator, true, optimizationSpace);
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param optimizationSpace context for {@link Variable}s
     * @param isWithOffset      whether to include an offset
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLinearEstimator(ExecutionOperator operator,
                                                                    boolean isWithOffset,
                                                                    OptimizationSpace optimizationSpace) {
        return createLinearEstimator(
                operator,
                RheemArrays.range(operator.getNumInputs()),
                RheemArrays.range(operator.getNumOutputs()),
                isWithOffset,
                optimizationSpace
        );
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param inputIndices      indices of {@link InputSlot}s for which {@link Variable}s should be created
     * @param outputIndices     indices of {@link OutputSlot}s for which {@link Variable}s should be created
     * @param isWithOffset      whether to include an offset
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createLinearEstimator(ExecutionOperator operator,
                                                                    int[] inputIndices,
                                                                    int[] outputIndices,
                                                                    boolean isWithOffset,
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
        Variable offsetVar = isWithOffset ? optimizationSpace.getOrCreateVariable(
                operator.getLoadProfileEstimatorConfigurationKey() + "->offset"
        ) : null;

        // Create the estimation function.
        final DynamicLoadEstimator.SinglePointEstimator singlePointEstimator = (individual, in, out) -> {
            double accu = isWithOffset ? offsetVar.getValue(individual) : 0d;
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
        if (isWithOffset) {
            sb.append("%s}");
        } else {
            sb.setLength(sb.length() - " + ".length());
            sb.append("}");
        }
        String juelTemplate = sb.toString();

        // Gather the employed variables.
        Collection<Variable> employedVariables = new LinkedList<>();
        employedVariables.addAll(Arrays.asList(inVars));
        employedVariables.addAll(Arrays.asList(outVars));
        if (isWithOffset) employedVariables.add(offsetVar);

        // Assemble the estimator.
        return new DynamicLoadProfileEstimator(
                operator.getLoadProfileEstimatorConfigurationKey(),
                operator.getNumInputs(),
                operator.getNumOutputs(),
                new DynamicLoadEstimator(singlePointEstimator, juelTemplate, employedVariables)
        );
    }

    /**
     * Create a {@link DynamicLoadProfileEstimator} that is linear in the input and output (including some offset).
     *
     * @param operator          the {@link ExecutionOperator} for that should be estimated
     * @param inputIndices      indices of {@link InputSlot}s for which {@link Variable}s should be created
     * @param outputIndices     indices of {@link OutputSlot}s for which {@link Variable}s should be created
     * @param isWithOffset      whether to include an offset
     * @param optimizationSpace context for {@link Variable}s
     * @return the {@link DynamicLoadProfileEstimator}
     */
    public static DynamicLoadProfileEstimator createQuadraticEstimator(ExecutionOperator operator,
                                                                       int[] inputIndices,
                                                                       int[] outputIndices,
                                                                       boolean isWithOffset,
                                                                       OptimizationSpace optimizationSpace) {
        // Create variables.
        Variable[] linearInVars = new Variable[inputIndices.length];
        Variable[] quadraticInVars = new Variable[inputIndices.length];
        for (int i = 0; i < inputIndices.length; i++) {
            int index = inputIndices[i];
            linearInVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getInput(index).getName()
            );
            quadraticInVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getInput(index).getName() + "^2"
            );
        }
        Variable[] linearOutVars = new Variable[outputIndices.length];
        Variable[] quadraticOutVars = new Variable[outputIndices.length];
        for (int i = 0; i < outputIndices.length; i++) {
            int index = outputIndices[i];
            linearOutVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getOutput(index).getName()
            );
            quadraticOutVars[i] = optimizationSpace.getOrCreateVariable(
                    operator.getLoadProfileEstimatorConfigurationKey() + "->" + operator.getOutput(index).getName() + "^2"
            );
        }

        Variable offsetVar = isWithOffset ? optimizationSpace.getOrCreateVariable(
                operator.getLoadProfileEstimatorConfigurationKey() + "->offset"
        ) : null;

        // Create the estimation function.
        final DynamicLoadEstimator.SinglePointEstimator singlePointEstimator = (individual, in, out) -> {
            double accu = isWithOffset ? offsetVar.getValue(individual) : 0d;
            for (int i = 0; i < inputIndices.length; i++) {
                accu += linearInVars[i].getValue(individual) * in[inputIndices[i]]
                        + quadraticInVars[i].getValue(individual) * in[inputIndices[i]] * in[inputIndices[i]];
            }
            for (int i = 0; i < outputIndices.length; i++) {
                accu += linearOutVars[i].getValue(individual) * out[outputIndices[i]]
                        + quadraticOutVars[i].getValue(individual) * out[outputIndices[i]] * out[outputIndices[i]];
            }
            return accu;
        };

        // Create the JUEL template.
        StringBuilder sb = new StringBuilder().append("${");
        for (int i = 0; i < inputIndices.length; i++) {
            sb.append("%s*in").append(inputIndices[i]).append(" + ")
                    .append("%s*in").append(inputIndices[i]).append("*in").append(inputIndices[i]).append(" + ");
        }
        for (int i = 0; i < outputIndices.length; i++) {
            sb.append("%s*out").append(outputIndices[i]).append(" + ")
                    .append("%s*out").append(inputIndices[i]).append("*out").append(inputIndices[i]).append(" + ");

        }
        if (isWithOffset) {
            sb.append("%s}");
        } else {
            sb.setLength(sb.length() - " + ".length());
            sb.append("}");
        }
        String juelTemplate = sb.toString();

        // Gather the employed variables.
        Collection<Variable> employedVariables = new LinkedList<>();
        for (int i = 0; i < linearInVars.length; i++) {
            employedVariables.add(linearInVars[i]);
            employedVariables.add(quadraticInVars[i]);
        }
        for (int i = 0; i < linearOutVars.length; i++) {
            employedVariables.add(linearOutVars[i]);
            employedVariables.add(quadraticOutVars[i]);
        }
        employedVariables.addAll(Arrays.asList(linearInVars));

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

    /**
     * Exposes a {@link LoadProfileEstimator} for {@link ExecutionOperator}s as a {@link DynamicLoadProfileEstimator} with the
     * caveat that the {@link ExecutionOperator} will not be available in the estimation process.
     *
     * @param loadProfileEstimator the {@link LoadProfileEstimator} or {@code null}
     * @return the {@link DynamicLoadProfileEstimator} or {@code null} if {@code loadProfileEstimator} is {@code null}
     */
    public static DynamicLoadProfileEstimator wrap(LoadProfileEstimator loadProfileEstimator) {
        return new DynamicLoadProfileEstimator("(none)", -1, -1, DynamicLoadEstimator.zeroLoad) {
            @Override
            public LoadProfile estimate(EstimationContext context) {
                return loadProfileEstimator.estimate(context);
            }

            @Override
            public Collection<Variable> getEmployedVariables() {
                return Collections.emptyList();
            }
        };
    }

    /**
     * Exposes a {@link LoadEstimator} for {@link ExecutionOperator}s as a {@link DynamicLoadEstimator} with the
     * caveat that the {@link ExecutionOperator} will not be available in the estimation process.
     *
     * @param loadEstimator the {@link LoadEstimator} or {@code null}
     * @return the {@link DynamicLoadEstimator} or {@code null} if {@code loadEstimator} is {@code null}
     */
    public static DynamicLoadEstimator wrap(LoadEstimator loadEstimator) {
        if (loadEstimator == null) return null;
        return new DynamicLoadEstimator(null, null, Collections.emptySet()) {
            @Override
            public LoadEstimate calculate(EstimationContext estimationContext) {
                return loadEstimator.calculate(estimationContext);
            }
        };
    }


}
