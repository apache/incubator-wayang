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

import java.util.Optional;

/**
 * Visitor (as in the Visitor Pattern) for {@link WayangPlan}s.
 */
public abstract class TopDownPlanVisitor<Payload, Return> {

    public Return process(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload) {
        final Optional<Return> returnOptional = this.prepareVisit(operator, fromOutputSlot, payload);
        if (returnOptional != null) {
            return returnOptional.orElse(null);
        }
        Return result = operator.accept(this, fromOutputSlot, payload);
        this.followUp(operator, fromOutputSlot, payload, result);

        return result;
    }

    protected abstract Optional<Return> prepareVisit(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload);

    protected abstract void followUp(Operator operator, OutputSlot<?> fromOutputSlot, Payload payload, Return result);

    /**
     * todo
     *
     * @param operatorAlternative
     */
    public abstract Return visit(OperatorAlternative operatorAlternative, OutputSlot<?> fromOutputSlot, Payload payload);

    public Return visit(Subplan subplan, OutputSlot<?> fromOutputSlot, Payload payload) {
        if (fromOutputSlot == null) {
            return subplan.getSink().accept(this, fromOutputSlot, payload);
        } else {
            final OutputSlot<Object> innerOutputSlot = subplan.traceOutput(fromOutputSlot).unchecked();
            return innerOutputSlot.getOwner().accept(this, innerOutputSlot, payload);
        }
    }

    /**
     * todo
     */
    public abstract Return visit(ActualOperator operator, OutputSlot<?> fromOutputSlot, Payload payload);

    protected Optional<Return> proceed(Operator operator, int inputIndex, Payload payload) {
        final InputSlot<Object> outerInputSlot = operator
                .getOutermostInputSlot(operator.getInput(inputIndex))
                .unchecked();
        final OutputSlot<Object> occupant = outerInputSlot.getOccupant();
        if (occupant != null) {
            return Optional.ofNullable(this.process(occupant.getOwner(), occupant, payload));
        } else {
            return null;
        }
    }

}
