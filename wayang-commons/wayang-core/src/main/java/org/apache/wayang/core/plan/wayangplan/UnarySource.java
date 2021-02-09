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
 * Abstract base class for sources with a single output.
 */
public abstract class UnarySource<T> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance that does not support broadcast {@link InputSlot}s.
     */
    public UnarySource(DataSetType<T> type) {
        this(type, false);
    }

    /**
     * Creates a new instance.
     */
    public UnarySource(DataSetType<T> type, boolean isSupportingBroadcastInputs) {
        super(0, 1, isSupportingBroadcastInputs);
        this.outputSlots[0] = new OutputSlot<>("out", this, type);
    }

    /**
     * Copies the given instance.
     *
     * @see UnarySource#UnarySource(DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected UnarySource(UnarySource<T> that) {
        super(that);
        this.outputSlots[0] = new OutputSlot<>("output", this, that.getType());
    }

    @SuppressWarnings("unchecked")
    public OutputSlot<T> getOutput() {
        return (OutputSlot<T>) this.getOutput(0);
    }

    public DataSetType<T> getType() {
        return this.getOutput().getType();
    }

}
