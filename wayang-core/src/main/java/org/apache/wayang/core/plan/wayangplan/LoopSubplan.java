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
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityPusher;
import org.apache.wayang.core.optimizer.cardinality.LoopSubplanCardinalityPusher;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Wraps a loop of {@link Operator}s.
 *
 * @see LoopIsolator
 */
public class LoopSubplan extends Subplan {

    private LoopHeadOperator loopHead;

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     */
    public static LoopSubplan wrap(LoopHeadOperator loopHead, List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs) {
        final OperatorContainer loopHeadContainer = loopHead.getContainer();
        final LoopSubplan loopSubplan = new LoopSubplan(loopHead, inputs, outputs);
        loopSubplan.setContainer(loopHeadContainer);
        return loopSubplan;
    }

    /**
     * Creates a new instance with the given operators. Initializes the {@link InputSlot}s and {@link OutputSlot}s,
     * steals existing connections, initializes the {@link #slotMapping}, and sets as inner {@link Operator}s' parent.
     *
     * @see #wrap(Operator, Operator)
     * @see #wrap(List, List, OperatorContainer)
     */
    private LoopSubplan(LoopHeadOperator loopHead, List<InputSlot<?>> inputs, List<OutputSlot<?>> outputs) {
        super(inputs, outputs);
        this.loopHead = loopHead;
    }

    /**
     * @see LoopHeadOperator#getNumExpectedIterations()
     */
    public int getNumExpectedIterations() {
        return this.loopHead.getNumExpectedIterations();
    }

    /**
     * @return the {@link LoopHeadOperator} of this instance
     */
    public LoopHeadOperator getLoopHead() {
        return this.loopHead;
    }

    @Override
    public Collection<OptimizationContext> getInnerInputOptimizationContext(
            InputSlot<?> innerInput,
            OptimizationContext outerOptimizationContext) {
        if (innerInput.getOwner() == this.loopHead) {
            // Retrieve the OptimizationContext of the first iteration -> this where we need to propagate to
            assert this.loopHead.getLoopInitializationInputs().contains(innerInput);
            return Collections.singleton(outerOptimizationContext.getNestedLoopContext(this).getInitialIterationContext());
        } else {
            return outerOptimizationContext.getNestedLoopContext(this).getIterationContexts();
        }
    }

    @Override
    public OptimizationContext getInnerOutputOptimizationContext(OptimizationContext outerOptimizationContext) {
        // Retrieve the OptimizationContext of the last iteration -> this where we need to propagate to
        return outerOptimizationContext.getNestedLoopContext(this).getFinalIterationContext();
    }


    @Override
    public CardinalityPusher getCardinalityPusher(Configuration configuration) {
        return new LoopSubplanCardinalityPusher(this, configuration);
    }

    @Override
    public void noteReplaced(Operator oldOperator, Operator newOperator) {
        super.noteReplaced(oldOperator, newOperator);
        if (oldOperator == this.loopHead) {
            this.loopHead = (LoopHeadOperator) newOperator;
        }
    }
}
