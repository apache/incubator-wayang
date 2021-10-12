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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.function.Function;
import org.apache.wayang.basic.operators.LoopOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Spark implementation of the {@link LoopOperator}.
 */
public class SparkLoopOperator<InputType, ConvergenceType>
        extends LoopOperator<InputType, ConvergenceType>
        implements SparkExecutionOperator {


    /**
     * Creates a new instance.
     */
    public SparkLoopOperator(DataSetType<InputType> inputType,
                             DataSetType<ConvergenceType> convergenceType,
                             PredicateDescriptor.SerializablePredicate<Collection<ConvergenceType>> criterionPredicate,
                             Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionPredicate, numExpectedIterations);
    }

    public SparkLoopOperator(DataSetType<InputType> inputType,
                             DataSetType<ConvergenceType> convergenceType,
                             PredicateDescriptor<Collection<ConvergenceType>> criterionDescriptor,
                             Integer numExpectedIterations) {
        super(inputType, convergenceType, criterionDescriptor, numExpectedIterations);
    }

    /**
     * Creates a copy of the given {@link LoopOperator}.
     *
     * @param that should be copied
     */
    public SparkLoopOperator(LoopOperator<InputType, ConvergenceType> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        ExecutionLineageNode executionLineageNode = new ExecutionLineageNode(operatorContext);
        executionLineageNode.addAtomicExecutionFromOperatorContext();

        final Function<Collection<ConvergenceType>, Boolean> stoppingCondition =
                sparkExecutor.getCompiler().compile(this.criterionDescriptor, this, operatorContext, inputs);

        boolean endloop = false;
        final Collection<ConvergenceType> convergenceCollection;
        final RddChannel.Instance input;
        switch (this.getState()) {
            case NOT_STARTED:
                assert inputs[INITIAL_INPUT_INDEX] != null;
                assert inputs[INITIAL_CONVERGENCE_INPUT_INDEX] != null;

                input = (RddChannel.Instance) inputs[INITIAL_INPUT_INDEX];
                JavaExecutionOperator.forward(inputs[INITIAL_CONVERGENCE_INPUT_INDEX], outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]);
                break;
            case RUNNING:
                assert inputs[ITERATION_INPUT_INDEX] != null;
                assert inputs[ITERATION_CONVERGENCE_INPUT_INDEX] != null;

                input = (RddChannel.Instance) inputs[ITERATION_INPUT_INDEX];
                convergenceCollection = ((CollectionChannel.Instance) inputs[ITERATION_CONVERGENCE_INPUT_INDEX]).provideCollection();
                executionLineageNode.addPredecessor(inputs[ITERATION_CONVERGENCE_INPUT_INDEX].getLineage());

                try {
                    endloop = stoppingCondition.call(convergenceCollection);
                } catch (Exception e) {
                    throw new WayangException(String.format("Executing %s's condition failed.", this), e);
                }
                JavaExecutionOperator.forward(inputs[ITERATION_CONVERGENCE_INPUT_INDEX], outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX]);
                break;
            default:
                throw new IllegalStateException(String.format("%s is finished, yet executed.", this));

        }

        if (endloop) {
            // final loop output
            sparkExecutor.forward(input, outputs[FINAL_OUTPUT_INDEX]);
            outputs[ITERATION_OUTPUT_INDEX] = null;
            outputs[ITERATION_CONVERGENCE_OUTPUT_INDEX] = null;
            this.setState(State.FINISHED);
        } else {
            outputs[FINAL_OUTPUT_INDEX] = null;
            sparkExecutor.forward(input, outputs[ITERATION_OUTPUT_INDEX]);
            this.setState(State.RUNNING);
        }

        return executionLineageNode.collectAndMark();
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkLoopOperator<>(
                this.getInputType(),
                this.getConvergenceType(),
                this.getCriterionDescriptor().getJavaImplementation(),
                this.getNumExpectedIterations()
        );
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.loop.load";
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                SparkExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.criterionDescriptor, configuration);
        return optEstimator;
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        switch (index) {
            case INITIAL_INPUT_INDEX:
            case ITERATION_INPUT_INDEX:
                return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        switch (index) {
            case ITERATION_OUTPUT_INDEX:
            case FINAL_OUTPUT_INDEX:
                // TODO: In this specific case, the actual output Channel is context-sensitive because we could forward Streams/Collections.
                return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
            case INITIAL_CONVERGENCE_INPUT_INDEX:
            case ITERATION_CONVERGENCE_INPUT_INDEX:
                return Collections.singletonList(CollectionChannel.DESCRIPTOR);
            default:
                throw new IllegalStateException(String.format("%s has no %d-th input.", this, index));
        }
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
