package org.apache.wayang.ml.encoding;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.util.Canonicalizer;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.ml.encoding.OneHotVector;

import java.util.Vector;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class OneHotEncoder implements Encoder {

    public static long[] encode(PlanImplementation plan) {
        OneHotVector result = new OneHotVector();

        if (plan.getOperators() == null) {
            return result.getEntries();
        }

        encodeTopologies(plan, result);
        encodeOperators(plan, result);
        encodeDataMovement(plan, result);
        encodeDataset(plan, result);

        return result.getEntries();
    }

    public static void encodeOperators(PlanImplementation plan, OneHotVector vector) {
        /*
         * Format:
         * ---- BEGIN OPERATOR ITERATION ----
         * 0 - total # instances
         * 1 - # instances in Java
         * 2 - # instances in Spark
         * 3 - # instances in Pipeline
         * 4 - # instances in Junction
         * 5 - # instances in Replicator
         * 6 - # instances in Loop
         * 7 - sum of UDF complexities
         * 8 - sum of input cardinalities
         * 9 - sum of output cardinalities
         */
        OptimizationContext optimizationContext = plan.getOptimizationContext();
        Canonicalizer<ExecutionOperator> operators = plan.getOperators();

        List<Object> distinctOperators = operators
            .stream()
            .map(
                (operator) -> operator
                    .getClass()
                    .getSuperclass()
            )
            .distinct()
            .collect(Collectors.toList());

        for (Object operator : distinctOperators) {
            // build the features
            long encodedOperator[] = new long[10];
            List<ExecutionOperator> executionOperators = operators
                .stream()
                .filter(op -> operator == op.getClass().getSuperclass())
                .collect(Collectors.toList());

            encodedOperator[0] = (long) executionOperators.size();

            for (ExecutionOperator executionOperator : executionOperators) {
                if (executionOperator.getPlatform() instanceof JavaPlatform)  {
                    encodedOperator[1] += 1;
                }

                if (executionOperator.getPlatform() instanceof SparkPlatform)  {
                    encodedOperator[2] += 1;
                }

                if (executionOperator instanceof UnaryToUnaryOperator)  {
                    encodedOperator[3] += 1;
                }

                if (executionOperator instanceof BinaryToUnaryOperator)  {
                    encodedOperator[4] += 1;
                }

                if (executionOperator.isLoopSubplan() || executionOperator.isLoopHead())  {
                    encodedOperator[6] += 1;
                }

                for (InputSlot<?> input: executionOperator.getAllInputs()) {
                    encodedOperator[8] += optimizationContext.getOperatorContext(executionOperator).getInputCardinality(input.getIndex()).getLowerEstimate();
                }

                for (OutputSlot<?> output: executionOperator.getAllOutputs()) {
                    encodedOperator[9] += optimizationContext.getOperatorContext(executionOperator).getOutputCardinality(output.getIndex()).getLowerEstimate();
                }
            }

            vector.addOperator(encodedOperator, ((Class) operator).getName());
        }
    }

    /*
     * Format:
     * 0 - number of operators
     * ---- BEGIN OPERATOR ITERATION ----
     * 0 - # instances in Java
     * 1 - # instances in Spark
     * 2 - sum of input cardinalities
     * 3 - sum of output cardinalities
     */
    public static void encodeDataMovement(
        PlanImplementation plan,
        OneHotVector vector
    ) {
        OptimizationContext optimizationContext = plan.getOptimizationContext();

        List<ExecutionTask> conversionTasks = plan
            .getJunctions()
            .values()
            .stream()
            .map(junction -> junction.getConversionTasks())
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        List<Object> distinctOperators = conversionTasks
            .stream()
            .map(task -> task.getOperator().getClass())
            .distinct()
            .collect(Collectors.toList());

        for (Object operator : distinctOperators) {
            long encodedOperator[] = new long[4];
            List<ExecutionOperator> executionOperators = conversionTasks
                .stream()
                .map(task -> task.getOperator())
                .filter(op -> operator == op.getClass())
                .collect(Collectors.toList());

            for (ExecutionOperator executionOperator : executionOperators) {
                if (executionOperator.getPlatform() instanceof JavaPlatform)  {
                    encodedOperator[0] += 1;
                }

                if (executionOperator.getPlatform() instanceof SparkPlatform)  {
                    encodedOperator[1] += 1;
                }

                OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(executionOperator);
                long inputCardinality = 0;
                long outputCardinality = 0;

                if (operatorContext == null) {
                    continue;
                }

                for (InputSlot<?> input : executionOperator.getAllInputs()) {
                    inputCardinality += optimizationContext.getOperatorContext(executionOperator).getInputCardinality(input.getIndex()).getLowerEstimate();
                }
                for (OutputSlot<?> output : executionOperator.getAllOutputs()) {
                    outputCardinality += optimizationContext.getOperatorContext(executionOperator).getOutputCardinality(output.getIndex()).getLowerEstimate();
                }

                encodedOperator[2] = inputCardinality;
                encodedOperator[3] = outputCardinality;
            }

            vector.addDataMovement(encodedOperator, ((Class) operator).getName());
        }
    }

    public static void encodeTopologies(PlanImplementation plan, OneHotVector vector) {
        long[] topologies = new long[OneHotVector.TOPOLOGIES_LENGTH];

        long replicatorCount = plan.
            getOperators()
            .stream()
            .filter((operator) -> operator.getAllOutputs().length > 1)
            .count();
        topologies[0] = replicatorCount;
        topologies[1] = getPipelineCount(plan);
        long junctionCounter = plan.
            getOperators()
            .stream()
            .filter((operator) -> operator.getAllInputs().length > 1)
            .count();
        topologies[2] = junctionCounter;
        topologies[3] = (long) plan.getLoopImplementations().size();

        vector.setTopologies(topologies);
    }

    private static long getPipelineCount(PlanImplementation plan) {
        long pipelineCount = 0;
        HashMap<Operator, Integer> visited = new HashMap<>();
        Canonicalizer<ExecutionOperator> operators = plan.getOperators();

        List<ExecutionOperator> startOperators = plan.getStartOperators();
        // traverse operators starting from each startOperator until
        // a junction target is found. Mark all as visited and increment
        // pipeline counter until no more visitable operators are existant.
        for (ExecutionOperator startOperator : startOperators) {
            pipelineCount += traverse(plan, startOperator, visited, 0, 0);
        }

        return pipelineCount;
    }

    private static long traverse(
        PlanImplementation plan,
        Operator current,
        HashMap<Operator, Integer> visited,
        int steps,
        int pipelineCount) {

        if (visited.containsKey(current)) {
            return pipelineCount;
        }

        visited.put(current, Integer.valueOf(1));
        OutputSlot<?>[] outputs = current.getAllOutputs();

        if (outputs.length == 0) {
            if (steps > 0) {
               pipelineCount++;
            }

            return pipelineCount;
        }

        // check if this junction output
        if (current.getAllInputs().length > 1) {
            if (steps > 1) {
                pipelineCount++;
            }

            for (int i = 0; i < outputs.length; i++) {
                Junction junction = plan.getJunction(outputs[i]);

                if (junction.getNumTargets() == 0) {
                    return pipelineCount;
                }

                for (InputSlot<?> input : junction.getTargetInputs()) {
                    Operator next = input.getOwner();
                    pipelineCount += traverse(plan, next, visited, 0, pipelineCount);
                }
            }
        }

        // check if this is replicator input
        if (current.getAllOutputs().length > 1) {
            if (steps > 1) {
                pipelineCount++;
            }

            for (int i = 0; i < outputs.length; i++) {
                Junction junction = plan.getJunction(outputs[i]);

                if (junction.getNumTargets() == 0) {
                    return pipelineCount;
                }

                for (InputSlot<?> input : junction.getTargetInputs()) {
                    Operator next = input.getOwner();
                    pipelineCount += traverse(plan, next, visited, 0, pipelineCount);
                }
            }
        }

        Junction junction = plan.getJunction(outputs[0]);

        if (junction.getNumTargets() == 0) {
            return pipelineCount;
        }
        Operator next = junction.getTargetInput(0).getOwner();

        return traverse(plan, next, visited, steps + 1, pipelineCount);
    }

    private static void encodeDataset(PlanImplementation plan, OneHotVector vector) {
        Configuration config = plan.getOptimizationContext().getConfiguration();
        vector.setDataset(config.getLongProperty("wayang.ml.tuple.average-size"));
    }
}
