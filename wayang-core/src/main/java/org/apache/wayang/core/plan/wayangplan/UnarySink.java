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

import org.apache.wayang.core.types.DataSetType;

/**
 * Abstract base-class for sinks with a single input.
 */
public abstract class UnarySink<T> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySink(DataSetType<T> type) {
        this(type, false);
    }

    /**
     * Creates a new instance.
     */
    public UnarySink(DataSetType<T> type, boolean isSupportingBroadcastInputs) {
        super(1, 0, isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, type);
    }

    /**
     * Copies the given instance.
     *
     * @see UnarySink#UnarySink(DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    public UnarySink(UnarySink<T> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getType());
    }

    @SuppressWarnings("unchecked")
    public InputSlot<T> getInput() {
        return (InputSlot<T>) this.getInput(0);
    }

    public DataSetType<T> getType() {
        return this.getInput().getType();
    }

}
