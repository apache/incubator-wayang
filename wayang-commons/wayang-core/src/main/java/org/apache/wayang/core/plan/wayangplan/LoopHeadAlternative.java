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
import org.apache.wayang.core.optimizer.cardinality.LoopHeadAlternativeCardinalityPusher;
import org.apache.wayang.core.util.WayangCollections;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Special {@link OperatorAlternative} for {@link LoopHeadOperator}s.
 */
public class LoopHeadAlternative extends OperatorAlternative implements LoopHeadOperator {

    private final LoopHeadOperator originalLoopHead;

    private Collection<OutputSlot<?>> loopBodyOutputs, finalLoopOutputs;

    private Collection<InputSlot<?>> loopBodyInputs, initializationInputs;

    /**
     * Creates a new instance.
     *
     * @param loopHead original {@link LoopHeadOperator} to be wrapped
     */
    LoopHeadAlternative(LoopHeadOperator loopHead) {
        super(loopHead);
        this.originalLoopHead = loopHead;
    }

    @Override
    public Alternative addAlternative(Operator alternativeOperator) {
        assert alternativeOperator.isLoopHead();
        assert !this.getAlternatives().isEmpty() || alternativeOperator == this.originalLoopHead;
        Alternative alternative = super.addAlternative(alternativeOperator);
        if (this.getAlternatives().size() == 1) {
            final Alternative originalAlternative = this.getAlternatives().get(0);
            this.loopBodyInputs = this.originalLoopHead.getLoopBodyInputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveUpstream).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.initializationInputs = this.originalLoopHead.getLoopInitializationInputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveUpstream).filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.loopBodyOutputs = this.originalLoopHead.getLoopBodyOutputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveDownstream)
                    .map(WayangCollections::getSingleOrNull)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            this.finalLoopOutputs = this.originalLoopHead.getFinalLoopOutputs().stream()
                    .map(originalAlternative.getSlotMapping()::resolveDownstream)
                    .map(WayangCollections::getSingleOrNull)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return alternative;
    }

    @Override
    public Collection<OutputSlot<?>> getLoopBodyOutputs() {
        return this.loopBodyOutputs;
    }

    @Override
    public Collection<OutputSlot<?>> getFinalLoopOutputs() {
        return this.finalLoopOutputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopBodyInputs() {
        return this.loopBodyInputs;
    }

    @Override
    public Collection<InputSlot<?>> getLoopInitializationInputs() {
        return this.initializationInputs;
    }

    @Override
    public Collection<InputSlot<?>> getConditionInputSlots() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<OutputSlot<?>> getConditionOutputSlots() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getNumExpectedIterations() {
        return this.originalLoopHead.getNumExpectedIterations();
    }

    @Override
    public CardinalityPusher getCardinalityPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopBodyInputs(),
                this.getLoopBodyOutputs(),
                (alternative, conf) -> alternative.getContainedOperator().getCardinalityPusher(conf),
                configuration
        );
    }

    @Override
    public CardinalityPusher getInitializationPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopInitializationInputs(),
                this.getLoopBodyOutputs(),
                (alternative, conf) -> ((LoopHeadOperator) alternative.getContainedOperator()).getInitializationPusher(conf),
                configuration
        );
    }

    @Override
    public CardinalityPusher getFinalizationPusher(Configuration configuration) {
        return new LoopHeadAlternativeCardinalityPusher(
                this,
                this.getLoopBodyInputs(),
                this.getFinalLoopOutputs(),
                (alternative, conf) -> ((LoopHeadOperator) alternative.getContainedOperator()).getFinalizationPusher(conf),
                configuration
        );
    }
}
