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

import java.util.Collection;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.types.DataSetType;

/**
 * Abstract class for inputs and outputs to operators.
 */
abstract public class Slot<T> {

    /**
     * Identifies this slot within its operator.
     */
    private final String name;

    /**
     * The operator that is being decorated by this slot.
     */
    private final Operator owner;

    /**
     * <i>Lazy initialized.</i> The index of this instance within its {@link #owner}.
     */
    protected int index = -1;

    /**
     * Type of data passed through this slot, expressed as a {@link DataSetType} so as to define not only the types of
     * elements that are passed but also capture their structure (e.g., flat, grouped, sorted, ...).
     */
    private final DataSetType<T> type;

    protected Slot(String name, Operator owner, DataSetType<T> type) {
        assert owner != null;
        this.name = name;
        this.owner = owner;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Operator getOwner() {
        return this.owner;
    }

    public DataSetType<T> getType() {
        return this.type;
    }

    /**
     * @return whether this is an {@link OutputSlot}
     */
    public boolean isOutputSlot() {
        return this instanceof OutputSlot;
    }

    /**
     * @return whether this is an input slot
     */
    public boolean isInputSlot() {
        return this instanceof InputSlot;
    }

    public boolean isCompatibleWith(Slot<?> that) {
        return this.type.equals(that.type);
    }

    @Override
    public String toString() {
        return String.format("%s@%s", this.name, this.owner == null ? "no owner" : this.owner.toString());
    }

    /**
     * @return the index of this slot within its owner
     * @throws IllegalStateException if this slot does not have an owner
     * @see #getOwner()
     */
    public abstract int getIndex() throws IllegalStateException;

    /**
     * Assign a {@link CardinalityEstimate} to this instance.
     *
     * @param cardinalityEstimate the {@link CardinalityEstimate} to assign
     * @deprecated This method does not do anything, anymore.
     */
    @Deprecated
    public void setCardinalityEstimate(CardinalityEstimate cardinalityEstimate) {
//        boolean isUpdate = this.cardinalityEstimate == null || !this.cardinalityEstimate.equals(cardinalityEstimate);
//        if (isUpdate) {
//            LoggerFactory.getLogger(this.getClass())
//                    .trace("Updating cardinality of {} from {} to {}.", this, this.cardinalityEstimate, cardinalityEstimate);
//            this.cardinalityEstimate = cardinalityEstimate;
//            this.mark();
//        }
    }

    /**
     * @deprecated These method does not do anything, anymore.
     */
    @Deprecated
    public CardinalityEstimate getCardinalityEstimate() {
        return new CardinalityEstimate(42, 42, 0.42);
    }
//
//    /**
//     * Set the mark of this instance. Used to communicate changes in the {@link CardinalityEstimate}.
//     * Will also be triggered by {@link #setCardinalityEstimate(CardinalityEstimate)}.
//     */
//    public void mark() {
//        this.isMarked = true;
//    }

//    /**
//     * Retrieves the mark of this instance, then clears it.
//     * Used to communicate changes in the {@link CardinalityEstimate}.
//     */
//    public boolean getAndClearMark() {
//        boolean wasMarked = this.isMarked;
//        this.isMarked = false;
//        return wasMarked;
//    }
//
//    public boolean isMarked() {
//        return this.isMarked;
//    }

    /**
     * Creates an {@code int[]} of the indices of the {@code slots}.
     */
    public static int[] toIndices(Collection<? extends Slot<?>> slots) {
        int[] indices = new int[slots.size()];
        int i = 0;
        for (Slot<?> slot : slots) indices[i++] = slot.getIndex();
        return indices;
    }
}
