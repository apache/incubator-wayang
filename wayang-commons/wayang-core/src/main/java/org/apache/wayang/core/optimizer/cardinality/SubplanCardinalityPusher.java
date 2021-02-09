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

package org.apache.wayang.core.optimizer.cardinality;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.CompositeOperator;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorContainer;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.Subplan;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * {@link CardinalityPusher} implementation for {@link Subplan}s (but not for {@link LoopSubplan}s!)
 */
public class SubplanCardinalityPusher extends CardinalityPusher {

    private final CardinalityEstimationTraversal traversal;

    /**
     * Create an instance for the given {@link Subplan}.
     *
     * @return the instance if it could be created
     */
    public static CardinalityPusher createFor(OperatorContainer container, Configuration configuration) {
        final CompositeOperator compositeOperator = container.toOperator();
        final InputSlot<?>[] outerInputs = compositeOperator.getAllInputs();
        final List<InputSlot<?>> innerInputs = Arrays.stream(outerInputs)
                .flatMap(inputSlot -> container.followInput(inputSlot).stream())
                .collect(Collectors.toList());
        final Collection<Operator> sourceOperators = compositeOperator.isSource() ?
                Collections.singleton(container.getSource()) : Collections.emptySet();
        final CardinalityEstimationTraversal traversal = CardinalityEstimationTraversal.createPushTraversal(
                innerInputs, sourceOperators, configuration);

        return new SubplanCardinalityPusher(traversal, compositeOperator);
    }

    /**
     * Creates a new instance.
     */
    private SubplanCardinalityPusher(CardinalityEstimationTraversal traversal, final CompositeOperator compositeOperator) {
        super(compositeOperator);
        assert !compositeOperator.isLoopSubplan() : String.format("%s is not suited for %s instances.",
                this.getClass().getSimpleName(), Subplan.class.getSimpleName());
        this.traversal = traversal;
    }

    @Override
    protected boolean canUpdate(OptimizationContext.OperatorContext opCtx) {
        // We always try to update because there might be internal updates.
        return true;
    }

    @Override
    protected void doPush(OptimizationContext.OperatorContext opCtx, Configuration configuration) {
        // Kick the traversal off.
        this.traversal.traverse(opCtx.getOptimizationContext(), configuration);

        // Pull the cardinalities for the OutputSlots.
        Subplan subplan = (Subplan) opCtx.getOperator();
        for (int outputIndex = 0; outputIndex < subplan.getNumOutputs(); outputIndex++) {
            final OutputSlot<?> innerOutput = subplan.traceOutput(subplan.getOutput(outputIndex));
            if (innerOutput != null) {
                final OptimizationContext.OperatorContext innerOperatorCtx =
                        opCtx.getOptimizationContext().getOperatorContext(innerOutput.getOwner());
                final CardinalityEstimate cardinality = innerOperatorCtx.getOutputCardinality(innerOutput.getIndex());
                opCtx.setOutputCardinality(outputIndex, cardinality);
            }
        }
    }

}
