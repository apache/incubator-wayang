/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml.encoding;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.wayang.basic.util.ComplexityUtils;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.util.Canonicalizer;
import org.apache.wayang.core.util.json.WayangJsonObj;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.wayang.ml.util.SampledCardinality;

public class OneHotEncoder {

    protected OneHotEncoder() {
    }

    public static final int PADDING_SIZE = 1;

    public static long[] encode(final PlanImplementation plan) {
        final OneHotVector result = new OneHotVector();

        if (plan.getOperators() == null) {
            return result.getEntries();
        }

        encodeTopologies(plan, result);
        encodeOperators(plan, result);
        encodeDataMovement(plan, result);
        encodeDataset(plan, result);

        return result.getEntries();
    }

    public static void encodeOperators(final PlanImplementation plan, final OneHotVector vector) {
        /*
         * Format: ---- BEGIN OPERATOR ITERATION ---- 0 - total # instances 1 - #
         * instances in Java 2 - # instances in Spark 3 - # instances in Pipeline 4 - #
         * instances in Junction 5 - # instances in Replicator 6 - # instances in Loop 7
         * - sum of UDF complexities 8 - sum of input cardinalities 9 - sum of output
         * cardinalities
         */
        final Canonicalizer<ExecutionOperator> operators = plan.getOperators();
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();
        final int platformsCount = platformMappings.size();

        final List<Class<?>> distinctOperators = operators.stream().map(operator -> operator.getClass().getSuperclass())
                .distinct().collect(Collectors.toList());

        for (final Class<?> operator : distinctOperators) {
            // build the features
            final long encodedOperator[] = new long[OneHotVector.OPERATOR_SIZE];
            final List<ExecutionOperator> executionOperators = operators.stream()
                    .filter(op -> operator == op.getClass().getSuperclass()).toList();

            encodedOperator[0] = (long) executionOperators.size();

            final List<SampledCardinality> operatorSamples = CardinalitySampler.samples.stream()
                    .filter(sample -> sample.getOperator().get("class").equals(operator.getName())).toList();

            final long inputCardinality = operatorSamples.stream().mapToLong(sample -> {
                long card = 0;
                for (final Object input : sample.getInputs()) {
                    card += ((WayangJsonObj) input).getLong("upperBound");
                }

                return card;
            }).sum();
            final long outputCardinality = operatorSamples.stream()
                    .mapToLong(sample -> sample.getOutput().getLong("cardinality")).sum();

            for (final ExecutionOperator executionOperator : executionOperators) {
                final Integer platformPosition = platformMappings
                        .get(executionOperator.getPlatform().getClass().getName());

                if (platformPosition == null) {
                    continue;
                }

                encodedOperator[platformPosition] += 1;

                if (executionOperator instanceof UnaryToUnaryOperator) {
                    encodedOperator[platformsCount + 1] += 1;
                }

                if (executionOperator instanceof BinaryToUnaryOperator) {
                    encodedOperator[platformsCount + 2] += 1;
                }

                if (executionOperator.isLoopSubplan() || executionOperator.isLoopHead()) {
                    encodedOperator[platformsCount + 3] += 1;
                }

                encodedOperator[platformPosition + 4] += ComplexityUtils.inferFromOperator(executionOperator).ordinal();
            }

            encodedOperator[platformsCount + 5] += inputCardinality;
            encodedOperator[platformsCount + 6] += outputCardinality;

            vector.addOperator(encodedOperator, operator.getName());
        }
    }

    /*
     * Format: ---- BEGIN OPERATOR ITERATION ---- 0 - # instances in Java 1 - #
     * instances in Spark 2 - sum of input cardinalities 3 - sum of output
     * cardinalities
     */
    public static void encodeDataMovement(final PlanImplementation plan, final OneHotVector vector) {
        final OptimizationContext optimizationContext = plan.getOptimizationContext();
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();
        final int platformsCount = platformMappings.size();

        final List<ExecutionTask> conversionTasks = plan.getJunctions().values().stream()
                .map(Junction::getConversionTasks).flatMap(Collection::stream).toList();

        final List<Class<?>> distinctOperators = conversionTasks.stream().map(task -> task.getOperator().getClass())
                .distinct().collect(Collectors.toList());

        for (final Class<?> operator : distinctOperators) {
            final long encodedOperator[] = new long[OneHotVector.CONVERSION_SIZE];
            final List<ExecutionOperator> executionOperators = conversionTasks.stream().map(ExecutionTask::getOperator)
                    .filter(op -> operator == op.getClass()).toList();

            for (final ExecutionOperator executionOperator : executionOperators) {
                final Integer platformPosition = platformMappings
                        .get(executionOperator.getPlatform().getClass().getName());

                if (platformPosition == null) {
                    continue;
                }

                encodedOperator[platformPosition] += 1;

                final OptimizationContext.OperatorContext operatorContext = optimizationContext
                        .getOperatorContext(executionOperator);

                if (operatorContext == null) {
                    continue;
                }

                final List<SampledCardinality> operatorSamples = CardinalitySampler.samples.stream().filter(
                        sample -> sample.getOperator().get("class").equals(executionOperator.getClass().getName()))
                        .toList();

                final long inputCardinality = operatorSamples.stream().mapToLong(sample -> {
                    long card = 0;
                    for (final Object input : sample.getInputs()) {
                        card += ((WayangJsonObj) input).getLong("upperBound");
                    }

                    return card;
                }).sum();
                final long outputCardinality = operatorSamples.stream()
                        .mapToLong(sample -> sample.getOutput().getLong("cardinality")).sum();

                encodedOperator[platformsCount] = inputCardinality;
                encodedOperator[platformsCount + 1] = outputCardinality;
            }

            vector.addDataMovement(encodedOperator, operator.getName());
        }
    }

    public static void encodeTopologies(final PlanImplementation plan, final OneHotVector vector) {
        final long[] topologies = new long[OneHotVector.TOPOLOGIES_LENGTH];

        final long replicatorCount = plan.getOperators().stream()
                .filter((operator) -> operator.getAllOutputs().length > 1).count();
        topologies[0] = replicatorCount;
        topologies[1] = getPipelineCount(plan);
        final long junctionCounter = plan.getOperators().stream()
                .filter((operator) -> operator.getAllInputs().length > 1).count();
        topologies[2] = junctionCounter;
        topologies[3] = (long) plan.getLoopImplementations().size();

        vector.setTopologies(topologies);
    }

    /*
     * Format: ---- BEGIN OPERATOR ITERATION ---- 0 - operator hashCode as long 1 -
     * sum of UDF complexities 2 - sum of input cardinalities 3 - sum of output
     * cardinalities (4 ... end) - one hot marking type of operator
     */
    public static long[] encodeOperator(final Operator operator, final OptimizationContext optimizationContext,
            final boolean encodeIds) {
        final List<SampledCardinality> operatorSamples = CardinalitySampler.samples.stream()
                .filter(sample -> sample.getOperator().get("class").equals(operator.getClass().getName())).toList();

        long inputCardinality = 0;
        long outputCardinality = 0;

        if (operatorSamples.size() == 0) {
            final OptimizationContext.OperatorContext operatorContext = optimizationContext
                    .getOperatorContext(operator);

            if (operatorContext != null) {
                for (final InputSlot<?> input : operator.getAllInputs()) {
                    final CardinalityEstimate card = operatorContext.getInputCardinality(input.getIndex());
                    if (card != null) {
                        inputCardinality += card.getLowerEstimate();
                    }
                }

                for (final OutputSlot<?> output : operator.getAllOutputs()) {
                    final CardinalityEstimate card = operatorContext.getOutputCardinality(output.getIndex());
                    if (card != null) {
                        outputCardinality += card.getLowerEstimate();
                    }
                }
            }
        } else {
            inputCardinality = operatorSamples.stream().mapToLong(sample -> {
                long card = 0;
                for (final Object input : sample.getInputs()) {
                    card += ((WayangJsonObj) input).getLong("upperBound");
                }

                return card;
            }).sum();
            outputCardinality = operatorSamples.stream().mapToLong(sample -> sample.getOutput().getLong("cardinality"))
                    .sum();
        }

        final HashMap<String, Integer> operatorMappings = OneHotMappings.getOperatorMapping();
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();

        final int operatorsCount = operatorMappings.size();
        final int platformsCount = platformMappings.size();

        final long[] result = new long[PADDING_SIZE + operatorsCount + platformsCount + 3];

        if (encodeIds) {
            result[0] = (long) new HashCodeBuilder(17, 37).append(operator.toString()).append(operator.getName())
                    .append(operator.getAllInputs().length).append(operator.getAllOutputs().length).toHashCode();
        }

        result[PADDING_SIZE + operatorsCount + platformsCount] = ComplexityUtils.inferFromOperator(operator).ordinal();
        result[PADDING_SIZE + operatorsCount + platformsCount + 1] = inputCardinality;
        result[PADDING_SIZE + operatorsCount + platformsCount + 2] = outputCardinality;

        final Integer operatorPosition = operatorMappings.get(operator.getClass().getName());
        result[1 + operatorPosition] = 1;

        return result;
    }

    public static long[] encodeOperator(final ExecutionOperator operator, final OptimizationContext optimizationContext,
            final boolean encodeIds) {
        final List<SampledCardinality> operatorSamples = CardinalitySampler.samples.stream()
                .filter(sample -> sample.getOperator().get("class").equals(operator.getClass().getName())).toList();

        long inputCardinality = 0;
        long outputCardinality = 0;

        if (operatorSamples.size() == 0) {
            final OptimizationContext.OperatorContext operatorContext = optimizationContext
                    .getOperatorContext(operator);

            if (operatorContext != null) {
                for (final InputSlot<?> input : operator.getAllInputs()) {
                    inputCardinality += operatorContext.getInputCardinality(input.getIndex()).getLowerEstimate();
                }

                for (final OutputSlot<?> output : operator.getAllOutputs()) {
                    outputCardinality += operatorContext.getOutputCardinality(output.getIndex()).getLowerEstimate();
                }
            }
        } else {
            inputCardinality = operatorSamples.stream().mapToLong(sample -> {
                long card = 0;
                for (final Object input : sample.getInputs()) {
                    card += ((WayangJsonObj) input).getLong("upperBound");
                }

                return card;
            }).sum();
            outputCardinality = operatorSamples.stream().mapToLong(sample -> sample.getOutput().getLong("cardinality"))
                    .sum();
        }

        final HashMap<String, Integer> operatorMappings = OneHotMappings.getOperatorMapping();
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();

        final int operatorsCount = operatorMappings.size();
        final int platformsCount = platformMappings.size();

        // Schema is: [ID, operator_1, ..., operator_N, platform_1, ..., platform_P,
        // udf, in_c, out_c]
        final long[] result = new long[PADDING_SIZE + operatorsCount + platformsCount + 3];

        if (encodeIds) {
            result[0] = (long) new HashCodeBuilder(17, 37).append(operator.toString()).append(operator.getName())
                    .append(operator.getAllInputs().length).append(operator.getAllOutputs().length).toHashCode();
        }

        result[PADDING_SIZE + operatorsCount + platformsCount] = ComplexityUtils.inferFromOperator(operator).ordinal();
        result[PADDING_SIZE + operatorsCount + platformsCount + 1] = inputCardinality;
        result[PADDING_SIZE + operatorsCount + platformsCount + 2] = outputCardinality;

        Integer operatorPosition = operatorMappings.get(operator.getClass().getSuperclass().getName());

        // Try to find a higher matching parent in the mappings
        if (operatorPosition == null) {
            operatorPosition = operatorMappings.get(operator.getClass().getSuperclass().getSuperclass().getName());
        }

        assert operatorPosition != null : operator.getClass().getSuperclass().getName() + " was not found in mappings";

        result[PADDING_SIZE + operatorPosition] = 1;

        final Integer platformPosition = platformMappings.get(operator.getPlatform().getClass().getName());
        result[PADDING_SIZE + operatorsCount + platformPosition] = 1;

        return result;
    }

    public static long[] encodeNullOperator() {
        final HashMap<String, Integer> operatorMappings = OneHotMappings.getOperatorMapping();
        final HashMap<String, Integer> platformMappings = OneHotMappings.getPlatformsMapping();

        final int operatorsCount = operatorMappings.size();
        final int platformsCount = platformMappings.size();
        final long[] result = new long[PADDING_SIZE + operatorsCount + platformsCount + 3];

        return result;
    }

    private static long getPipelineCount(final PlanImplementation plan) {
        long pipelineCount = 0;
        final HashMap<Operator, Integer> visited = new HashMap<>();
        final List<ExecutionOperator> startOperators = plan.getStartOperators();

        // traverse operators starting from each startOperator until
        // a junction target is found. Mark all as visited and increment
        // pipeline counter until no more visitable operators are existant.
        for (final ExecutionOperator startOperator : startOperators) {
            pipelineCount += traverse(plan, startOperator, visited, 0, 0);
        }

        return pipelineCount;
    }

    private static long traverse(final PlanImplementation plan, final Operator current,
            final HashMap<Operator, Integer> visited, final int steps, long pipelineCount) {

        if (visited.containsKey(current)) {
            return pipelineCount;
        }

        visited.put(current, Integer.valueOf(1));
        final OutputSlot<?>[] outputs = current.getAllOutputs();

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
                final Junction junction = plan.getJunction(outputs[i]);

                if (junction.getNumTargets() == 0) {
                    return pipelineCount;
                }

                for (final InputSlot<?> input : junction.getTargetInputs()) {
                    final Operator next = input.getOwner();
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
                final Junction junction = plan.getJunction(outputs[i]);

                if (junction.getNumTargets() == 0) {
                    return pipelineCount;
                }

                for (final InputSlot<?> input : junction.getTargetInputs()) {
                    final Operator next = input.getOwner();
                    pipelineCount += traverse(plan, next, visited, 0, pipelineCount);
                }
            }
        }

        final Junction junction = plan.getJunction(outputs[0]);

        if (junction.getNumTargets() == 0) {
            return pipelineCount;
        }
        final Operator next = junction.getTargetInput(0).getOwner();

        return traverse(plan, next, visited, steps + 1, pipelineCount);
    }

    private static void encodeDataset(final PlanImplementation plan, final OneHotVector vector) {
        vector.setDataset(100l);
    }
}
