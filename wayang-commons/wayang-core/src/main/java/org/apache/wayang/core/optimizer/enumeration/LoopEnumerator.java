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

package org.apache.wayang.core.optimizer.enumeration;

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.InputSlot;
import org.apache.wayang.core.plan.wayangplan.LoopHeadOperator;
import org.apache.wayang.core.plan.wayangplan.LoopSubplan;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.Junction;
import org.apache.wayang.core.util.OneTimeExecutable;
import org.apache.wayang.core.util.Tuple;
import org.apache.logging.log4j.LogManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Enumerator for {@link LoopSubplan}s.
 */
public class LoopEnumerator extends OneTimeExecutable {

    private final OptimizationContext.LoopContext loopContext;

    private final PlanEnumerator planEnumerator;

    private PlanEnumeration loopEnumeration;

    public LoopEnumerator(PlanEnumerator planEnumerator, OptimizationContext.LoopContext loopContext) {
        this.planEnumerator = planEnumerator;
        this.loopContext = loopContext;
    }

    public PlanEnumeration enumerate() {
        this.tryExecute();
        return this.loopEnumeration;
    }

    @Override
    protected void doExecute() {
        // Create aggregate iteration contexts.
        OptimizationContext aggregateContext = this.loopContext.getAggregateContext();
        LoopSubplan loop = this.loopContext.getLoop();

        // Create the end result.
        this.loopEnumeration = new PlanEnumeration();
        for (OutputSlot<?> loopOutput : loop.getAllOutputs()) {
            if (loopOutput.getOccupiedSlots().isEmpty()) {
                this.loopEnumeration.getServingOutputSlots().add(new Tuple<>(loopOutput, null));
            } else {
                for (InputSlot<?> input : loopOutput.getOccupiedSlots()) {
                    this.loopEnumeration.getServingOutputSlots().add(new Tuple<>(loopOutput, input));
                }
            }
        }
        for (InputSlot<?> loopInput : loop.getAllInputs()) {
            this.loopEnumeration.getRequestedInputSlots().add(loopInput);
        }

        // Enumerate the loop body (for now, only a single loop body).
        final PlanEnumerator loopBodyEnumerator =
                this.planEnumerator.forkFor(this.loopContext.getLoop().getLoopHead(), aggregateContext);
        final PlanEnumeration loopBodyEnumeration = loopBodyEnumerator.enumerate(true);

        // Enumerate feedback connections.
        this.addFeedbackConnections(loopBodyEnumeration, null, aggregateContext);

        // Wrap each PlanImplementation in a new PlanImplementation that subsumes the whole loop instead of iterations.
        for (PlanImplementation loopBodyImplementation : loopBodyEnumeration.getPlanImplementations()) {
            final LoopImplementation loopImplementation = new LoopImplementation(this.loopContext.getLoop());
            loopImplementation.addIterationEnumeration(
                    this.loopContext.getLoop().getNumExpectedIterations(), loopBodyImplementation
            );
            final PlanImplementation planImplementation = new PlanImplementation(
                    this.loopEnumeration,
                    new HashMap<>(1),
                    this.loopContext.getOptimizationContext()
            );
            planImplementation.addLoopImplementation(loop, loopImplementation);
            this.loopEnumeration.add(planImplementation);
        }
    }

    /**
     * Adds feedback {@link Junction}s for all {@link PlanImplementation}s in the {@code curBodyEnumeration}.
     *
     * @param curBodyEnumeration       a {@link PlanEnumeration} within which the feedback {@link Junction}s should be added
     * @param successorBodyEnumeration implemetns the successor iteration
     * @param optimizationContext      used for the createion of {@code curBodyEnumeration}
     */
    private void addFeedbackConnections(PlanEnumeration curBodyEnumeration,
                                        PlanEnumeration successorBodyEnumeration,
                                        OptimizationContext optimizationContext) {
        assert successorBodyEnumeration == null : "Multiple loop enumerations not supported, yet.";

        // Find the LoopHeadOperator.
        LoopHeadOperator loopHead = this.loopContext.getLoop().getLoopHead();

        // Go through all loop body InputSlots.
        for (Iterator<PlanImplementation> iterator = curBodyEnumeration.getPlanImplementations().iterator(); iterator.hasNext(); ) {
            PlanImplementation loopImpl = iterator.next();
            for (InputSlot<?> loopBodyInput : loopHead.getLoopBodyInputs()) {
                final OutputSlot<?> occupant = loopBodyInput.getOccupant();
                if (occupant == null) continue;
                if (!this.addFeedbackConnection(loopImpl, occupant, loopBodyInput, optimizationContext)) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Adds feedback {@link Junction}s for a {@link PlanImplementation}.
     *
     * @param loopImpl      to which the {@link Junction} should be added
     * @param occupant      {@link OutputSlot} (abstract) that provides feedback connections
     * @param loopBodyInput {@link InputSlot} (abstract) which takes the feedback connection
     * @return whether the adding the feedback connection was successful
     */
    private boolean addFeedbackConnection(PlanImplementation loopImpl,
                                          OutputSlot<?> occupant,
                                          InputSlot<?> loopBodyInput,
                                          OptimizationContext optimizationContext) {
        final List<InputSlot<?>> execLoopBodyInputs = new ArrayList<>(loopImpl.findExecutionOperatorInputs(loopBodyInput));
        final Collection<OutputSlot<?>> execOutputs = loopImpl.findExecutionOperatorOutput(occupant);
        for (OutputSlot<?> execOutput : execOutputs) {
            final Junction existingJunction = loopImpl.getJunction(execOutput);
            if (existingJunction != null) {
                LogManager.getLogger(this.getClass()).debug(
                        "Need to override existing {} for {} while closing loop.",
                        existingJunction, execOutput
                );
                execLoopBodyInputs.addAll(existingJunction.getTargetInputs());
            }
            final Junction junction = optimizationContext.getChannelConversionGraph().findMinimumCostJunction(
                    execOutput, execLoopBodyInputs, optimizationContext, false
            );
            if (junction == null) return false;

            loopImpl.putJunction(execOutput, junction);
        }
        return true;
    }

}
