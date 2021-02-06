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
 * This operator has a single input and a single output.
 */
public abstract class UnaryToUnaryOperator<InputType, OutputType> extends OperatorBase implements ElementaryOperator {

    /**
     * Creates a new instance.
     */
    protected UnaryToUnaryOperator(DataSetType<InputType> inputType,
                                   DataSetType<OutputType> outputType,
                                   boolean isSupportingBroadcastInputs) {
        super(1, 1, isSupportingBroadcastInputs);
        this.inputSlots[0] = new InputSlot<>("in", this, inputType);
        this.outputSlots[0] = new OutputSlot<>("out", this, outputType);
    }

    /**
     * Copies the given instance.
     *
     * @see UnaryToUnaryOperator#UnaryToUnaryOperator(DataSetType, DataSetType, boolean)
     * @see OperatorBase#OperatorBase(OperatorBase)
     */
    protected UnaryToUnaryOperator(UnaryToUnaryOperator<InputType, OutputType> that) {
        super(that);
        this.inputSlots[0] = new InputSlot<>("in", this, that.getInputType());
        this.outputSlots[0] = new OutputSlot<>("out", this, that.getOutputType());
    }

    @SuppressWarnings("unchecked")
    public InputSlot<InputType> getInput() {
        return (InputSlot<InputType>) this.getInput(0);
    }

    @SuppressWarnings("unchecked")
    public OutputSlot<OutputType> getOutput() {
        return (OutputSlot<OutputType>) this.getOutput(0);
    }

    public DataSetType<InputType> getInputType() {
        return this.getInput().getType();
    }

    public DataSetType<OutputType> getOutputType() {
        return this.getOutput().getType();
    }
}
