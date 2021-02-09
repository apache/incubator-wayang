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

import org.apache.commons.lang3.Validate;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;


/**
 * This operator returns the cartesian product of elements of input datasets.
 */
public class JoinOperator<InputType0, InputType1, Key>
        extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    private static <InputType0, InputType1> DataSetType<Tuple2<InputType0, InputType1>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    protected final TransformationDescriptor<InputType0, Key> keyDescriptor0;

    protected final TransformationDescriptor<InputType1, Key> keyDescriptor1;

    public JoinOperator(FunctionDescriptor.SerializableFunction<InputType0, Key> keyExtractor0,
                        FunctionDescriptor.SerializableFunction<InputType1, Key> keyExtractor1,
                        Class<InputType0> input0Class,
                        Class<InputType1> input1Class,
                        Class<Key> keyClass) {
        this(
                new TransformationDescriptor<>(keyExtractor0, input0Class, keyClass),
                new TransformationDescriptor<>(keyExtractor1, input1Class, keyClass)
        );
    }

    public JoinOperator(TransformationDescriptor<InputType0, Key> keyDescriptor0,
                        TransformationDescriptor<InputType1, Key> keyDescriptor1) {
        super(DataSetType.createDefault(keyDescriptor0.getInputType()),
                DataSetType.createDefault(keyDescriptor1.getInputType()),
                JoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }
    public JoinOperator(TransformationDescriptor<InputType0, Key> keyDescriptor0,
                        TransformationDescriptor<InputType1, Key> keyDescriptor1,
                        DataSetType<InputType0> inputType0,
                        DataSetType<InputType1> inputType1) {
        super(inputType0, inputType1, JoinOperator.createOutputDataSetType(), true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
    }


    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JoinOperator(JoinOperator<InputType0, InputType1, Key> that) {
        super(that);
        this.keyDescriptor0 = that.getKeyDescriptor0();
        this.keyDescriptor1 = that.getKeyDescriptor1();
    }

    public TransformationDescriptor<InputType0, Key> getKeyDescriptor0() {
        return this.keyDescriptor0;
    }

    public TransformationDescriptor<InputType1, Key> getKeyDescriptor1() {
        return this.keyDescriptor1;
    }


    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        // The current idea: We assume, we have a foreign-key like join
        // TODO: Find a better estimator.
        return Optional.of(new DefaultCardinalityEstimator(
                .5d, 2, this.isSupportingBroadcastInputs(),
                inputCards -> 3 * Math.max(inputCards[0], inputCards[1])
        ));
    }
}
