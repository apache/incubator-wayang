package org.apache.wayang.ml.encoding;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.OperatorAlternative;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.util.Canonicalizer;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.ml.encoding.OneHotVector;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.wayang.ml.util.Platforms;
import org.apache.wayang.ml.util.SampledCardinality;
import org.apache.wayang.basic.util.Udf;

import java.util.Vector;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;
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
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();
        int platformsCount = platformMappings.size();

        List<Object> distinctOperators = operators
            .stream()
            .map(
                (operator) -> operator
                    .getClass()
                    .getSuperclass()
            )
            .distinct()
            .collect(Collectors.toList());

        for (final Object operator : distinctOperators) {
            // build the features
            long encodedOperator[] = new long[OneHotVector.OPERATOR_SIZE];
            List<ExecutionOperator> executionOperators = operators
                .stream()
                .filter(op -> operator == op.getClass().getSuperclass())
                .collect(Collectors.toList());

            encodedOperator[0] = (long) executionOperators.size();

            List<SampledCardinality> operatorSamples = CardinalitySampler.samples
                .stream()
                .filter(sample -> {
                    return sample.getOperator().get("class").equals(((Class) operator).getName());
                }).collect(Collectors.toList());

            long inputCardinality = operatorSamples.stream()
                .mapToLong(sample -> {
                    long card = 0;
                    for (Object input : sample.getInputs()) {
                        card += ((WayangJsonObj) input).getLong("upperBound");
                    }

                    return card;
                })
                .sum();
            long outputCardinality = operatorSamples.stream().mapToLong(sample -> sample.getOutput().getLong("cardinality")).sum();

            for (ExecutionOperator executionOperator : executionOperators) {
                Integer platformPosition = platformMappings.get(executionOperator.getPlatform().getClass().getName());

                if (platformPosition == null) {
                    continue;
                }

                encodedOperator[platformPosition] += 1;

                if (executionOperator instanceof UnaryToUnaryOperator)  {
                    encodedOperator[platformsCount + 1] += 1;
                }

                if (executionOperator instanceof BinaryToUnaryOperator)  {
                    encodedOperator[platformsCount + 2] += 1;
                }

                if (executionOperator.isLoopSubplan() || executionOperator.isLoopHead())  {
                    encodedOperator[platformsCount + 3] += 1;
                }

                encodedOperator[platformPosition + 4] += Udf.getComplexity(executionOperator).ordinal();
            }

            encodedOperator[platformsCount + 5] += inputCardinality;
            encodedOperator[platformsCount + 6] += outputCardinality;

            vector.addOperator(encodedOperator, ((Class) operator).getName());
        }
    }

    /*
     * Format:
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
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();
        int platformsCount = platformMappings.size();

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
            long encodedOperator[] = new long[OneHotVector.CONVERSION_SIZE];
            List<ExecutionOperator> executionOperators = conversionTasks
                .stream()
                .map(task -> task.getOperator())
                .filter(op -> operator == op.getClass())
                .collect(Collectors.toList());

            for (final ExecutionOperator executionOperator : executionOperators) {
                Integer platformPosition = platformMappings.get(executionOperator.getPlatform().getClass().getName());

                if (platformPosition == null) {
                    continue;
                }

                encodedOperator[platformPosition] += 1;

                OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(executionOperator);

                if (operatorContext == null) {
                    continue;
                }

                List<SampledCardinality> operatorSamples = CardinalitySampler.samples
                    .stream()
                    .filter(sample -> {
                        return sample.getOperator().get("class").equals(executionOperator.getClass().getName());
                    }).collect(Collectors.toList());

                long inputCardinality = operatorSamples.stream()
                    .mapToLong(sample -> {
                        long card = 0;
                        for (Object input : sample.getInputs()) {
                            card += ((WayangJsonObj) input).getLong("upperBound");
                        }

                        return card;
                    })
                    .sum();
                long outputCardinality = operatorSamples.stream().mapToLong(sample -> sample.getOutput().getLong("cardinality")).sum();

                encodedOperator[platformsCount] = inputCardinality;
                encodedOperator[platformsCount + 1] = outputCardinality;
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
        //vector.setDataset(config.getLongProperty("wayang.ml.tuple.average-size"));
        vector.setDataset(100l);
    }

    /*
     * Format:
     * ---- BEGIN OPERATOR ITERATION ----
     * 0 - operator hashCode as long
     * 1 - sum of UDF complexities
     * 2 - sum of input cardinalities
     * 3 - sum of output cardinalities
     * (4 ... end) - one hot marking type of operator
     */
    public static long[] encodeOperator(Operator operator) {
        List<SampledCardinality> operatorSamples = CardinalitySampler.samples
            .stream()
            .filter(sample -> {
                return sample.getOperator().get("class").equals(operator.getClass().getName());
            }).collect(Collectors.toList());

        OptimizationContext optimizationContext = OneHotMappings.getOptimizationContext();
        long inputCardinality = 0;
        long outputCardinality = 0;

        if (operatorSamples.size() == 0) {
            for (InputSlot<?> input: operator.getAllInputs()) {
                inputCardinality += optimizationContext.getOperatorContext(operator).getInputCardinality(input.getIndex()).getLowerEstimate();
            }

            for (OutputSlot<?> output: operator.getAllOutputs()) {
                outputCardinality += optimizationContext.getOperatorContext(operator).getOutputCardinality(output.getIndex()).getLowerEstimate();
            }
        } else {
            inputCardinality = operatorSamples.stream()
                .mapToLong(sample -> {
                    long card = 0;
                    for (Object input : sample.getInputs()) {
                        card += ((WayangJsonObj) input).getLong("upperBound");
                    }

                    return card;
                })
                .sum();
            outputCardinality = operatorSamples.stream()
                .mapToLong(sample -> sample.getOutput().getLong("cardinality"))
                .sum();
        }

        HashMap<String, Integer> operatorMappings = OneHotMappings.getInstance().getOperatorMapping();
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();

        int operatorsCount = operatorMappings.size();
        int platformsCount = platformMappings.size();
        long[] result = new long[operatorsCount + platformsCount + 3];

        result[0] = (long) new HashCodeBuilder(17, 37)
            .append(operator.toString())
            .append(operator.getAllInputs())
            .append(operator.getAllOutputs())
            .toHashCode();
        result[operatorsCount + platformsCount] = Udf.getComplexity(operator).ordinal();
        result[operatorsCount + platformsCount + 1] = inputCardinality;
        result[operatorsCount + platformsCount + 2] = outputCardinality;

        Integer operatorPosition = operatorMappings.get(operator.getClass().getName());
        result[operatorPosition] = 1;

        return result;
    }

    public static long[] encodeOperator(ExecutionOperator operator) {
        List<SampledCardinality> operatorSamples = CardinalitySampler.samples
            .stream()
            .filter(sample -> {
                return sample.getOperator().get("class").equals(operator.getClass().getName());
            }).collect(Collectors.toList());

        OptimizationContext optimizationContext = OneHotMappings.getOptimizationContext();
        long inputCardinality = 0;
        long outputCardinality = 0;

        if (operatorSamples.size() == 0) {
            OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(operator);

            if (operatorContext != null) {
                for (InputSlot<?> input: operator.getAllInputs()) {
                    inputCardinality += operatorContext.getInputCardinality(input.getIndex()).getLowerEstimate();
                }

                for (OutputSlot<?> output: operator.getAllOutputs()) {
                    outputCardinality += operatorContext.getOutputCardinality(output.getIndex()).getLowerEstimate();
                }
            }
        } else {
            inputCardinality = operatorSamples.stream()
                .mapToLong(sample -> {
                    long card = 0;
                    for (Object input : sample.getInputs()) {
                        card += ((WayangJsonObj) input).getLong("upperBound");
                    }

                    return card;
                })
                .sum();
            outputCardinality = operatorSamples.stream()
                .mapToLong(sample -> sample.getOutput().getLong("cardinality"))
                .sum();
        }

        HashMap<String, Integer> operatorMappings = OneHotMappings.getInstance().getOperatorMapping();
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();

        int operatorsCount = operatorMappings.size();
        int platformsCount = platformMappings.size();
        long[] result = new long[operatorsCount + platformsCount + 3];

        result[0] = (long) new HashCodeBuilder(17, 37)
            .append(operator.toString())
            .append(operator.getAllInputs())
            .append(operator.getAllOutputs())
            .toHashCode();

        result[operatorsCount + platformsCount] = Udf.getComplexity(operator).ordinal();
        result[operatorsCount + platformsCount + 1] = inputCardinality;
        result[operatorsCount + platformsCount + 2] = outputCardinality;

        Integer operatorPosition = operatorMappings.get(operator.getClass().getSuperclass().getName());
        assert operatorPosition != null : operator.getClass().getSuperclass().getName() + " was not found in mappings";
        result[operatorPosition] = 1;

        Integer platformPosition = platformMappings.get(operator.getPlatform().getClass().getName());
        result[operatorsCount + platformPosition] = 1;

        return result;
    }

    public static long[] encodeNullOperator() {
        HashMap<String, Integer> operatorMappings = OneHotMappings.getInstance().getOperatorMapping();
        HashMap<String, Integer> platformMappings = OneHotMappings.getInstance().getPlatformsMapping();

        int operatorsCount = operatorMappings.size();
        int platformsCount = platformMappings.size();
        long[] result = new long[operatorsCount + platformsCount + 3];

        result[operatorsCount - 1] = 1;

        return result;
    }
}
