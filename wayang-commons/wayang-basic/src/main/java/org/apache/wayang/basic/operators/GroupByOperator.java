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

package org.apache.wayang.basic.operators;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * This is the auxiliary GroupBy operator, i.e., it behaves differently depending on its context. If it is followed
 * by a {@link ReduceOperator} (and akin), it turns that one into a {@link ReduceByOperator}. Otherwise, it corresponds to a
 * {@link MaterializedGroupByOperator}.
 *
 * @see MaterializedGroupByOperator
 * @see ReduceOperator
 */
public class GroupByOperator<Input, Key> extends UnaryToUnaryOperator<Input, Iterable<Input>> {

    protected final TransformationDescriptor<Input, Key> keyDescriptor;

    /**
     * Creates a new instance.
     *
     * @param keyFunction describes how to extract the key from data units
     * @param typeClass   class of the data quanta to be grouped
     * @param keyClass    class of the extracted keys
     */
    public GroupByOperator(FunctionDescriptor.SerializableFunction<Input, Key> keyFunction,
                           Class<Input> typeClass,
                           Class<Key> keyClass) {
        this(new TransformationDescriptor<>(keyFunction, typeClass, keyClass));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     */
    public GroupByOperator(TransformationDescriptor<Input, Key> keyDescriptor) {
        this(keyDescriptor,
                DataSetType.createDefault(keyDescriptor.getInputType()),
                DataSetType.createGrouped(keyDescriptor.getInputType()));
    }

    /**
     * Creates a new instance.
     *
     * @param keyDescriptor describes the key w.r.t. to the processed data units
     * @param inputType     class of the input types (i.e., type of {@link #getInput()}
     * @param outputType    class of the output types (i.e., type of {@link #getOutput()}
     */
    public GroupByOperator(TransformationDescriptor<Input, Key> keyDescriptor,
                           DataSetType<Input> inputType, DataSetType<Iterable<Input>> outputType) {
        super(inputType, outputType, false);
        this.keyDescriptor = keyDescriptor;
    }


    public GroupByOperator(GroupByOperator<Input, Key> that){
        super(that);
        this.keyDescriptor = that.keyDescriptor;
    }

    public TransformationDescriptor<Input, Key> getKeyDescriptor() {
        return this.keyDescriptor;
    }


}
