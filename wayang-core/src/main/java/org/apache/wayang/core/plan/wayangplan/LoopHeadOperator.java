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

package org.apache.wayang.core.plan.wayangplan;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityPusher;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityPusher;

import java.util.Collection;

/**
 * Head of a {@link LoopSubplan}.
 */
public interface LoopHeadOperator extends Operator {


    /**
     * If this instance is the head of a loop, then return these {@link OutputSlot}s that go into the loop body (as
     * opposed to the {@link OutputSlot}s that form the final result of the iteration).
     *
     * @return the loop body-bound {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getLoopBodyOutputs();

    /**
     * If this instance is the head of a loop, then return these {@link OutputSlot}s that form the final result of
     * the iteration.
     *
     * @return the loop-terminal {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getFinalLoopOutputs();

    /**
     * If this instance is the head of a loop, then return these {@link InputSlot}s that are fed from the loop body (as
     * opposed to the {@link InputSlot}s that initialize the loop).
     *
     * @return the loop body-bound {@link InputSlot}s
     */
    Collection<InputSlot<?>> getLoopBodyInputs();

    /**
     * If this instance is the head of a loop, then return these {@link InputSlot}s that initialize the loop.
     *
     * @return the initialization {@link InputSlot}s
     */
    Collection<InputSlot<?>> getLoopInitializationInputs();

    /**
     * Retrieve those {@link InputSlot}s that are required to evaluate the loop condition.
     *
     * @return the condition {@link InputSlot}s
     */
    Collection<InputSlot<?>> getConditionInputSlots();

    /**
     * Retrieve those {@link OutputSlot}s that forward the {@link #getConditionInputSlots()}.
     *
     * @return the condition {@link OutputSlot}s
     */
    Collection<OutputSlot<?>> getConditionOutputSlots();



    /**
     * @return a number of expected iterations; not necessarily the actual value
     */
    int getNumExpectedIterations();

    /**
     * Get the {@link CardinalityPusher} implementation for the intermediate iterations.
     */
    @Override
    default CardinalityPusher getCardinalityPusher(final Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * Get the {@link CardinalityPusher} implementation for the initial iteration.
     */
    default CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopInitializationInputs()),
                Slot.toIndices(this.getLoopBodyOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * Get the {@link CardinalityPusher} implementation for the final iteration.
     */
    default CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new DefaultCardinalityPusher(this,
                Slot.toIndices(this.getLoopBodyInputs()),
                Slot.toIndices(this.getFinalLoopOutputs()),
                configuration.getCardinalityEstimatorProvider());
    }

    /**
     * <i>Optional operation. Only {@link ExecutionOperator}s need to implement this method.</i>
     * @return the current {@link State} of this instance
     */
    default State getState() {
        assert !this.isExecutionOperator();
        throw new UnsupportedOperationException();
    }

    /**
     * <i>Optional operation. Only {@link ExecutionOperator}s need to implement this method.</i> Sets the {@link State} of this instance.
     */
    default void setState(State state) {
        assert !this.isExecutionOperator();
        throw new UnsupportedOperationException();
    }

    /**
     * {@link LoopHeadOperator}s can be stateful because they might be executed mulitple times. This {@code enum}
     * expresses this state.
     */
    enum State {
        /**
         * The {@link LoopHeadOperator} has not been executed yet.
         */
        NOT_STARTED,

        /**
         * The {@link LoopHeadOperator} has been executed. However, the last execution was not the ultimate one.
         */
        RUNNING,

        /**
         * The {@link LoopHeadOperator} has been executed ultimately.
         */
        FINISHED
    }
}
