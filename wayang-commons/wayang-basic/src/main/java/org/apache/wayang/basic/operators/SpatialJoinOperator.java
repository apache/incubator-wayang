/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

/**
 * This operator returns a new dataset after joining the input tables using the predicate.
 */
public class SpatialJoinOperator<InputType0, InputType1> extends BinaryToUnaryOperator<InputType0, InputType1, Tuple2<InputType0, InputType1>> {

    private static <InputType0, InputType1> DataSetType<Tuple2<InputType0, InputType1>> createOutputDataSetType() {
        return DataSetType.createDefaultUnchecked(Tuple2.class);
    }

    protected final TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0;

    protected final TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1;

    protected final SpatialPredicate predicateType;

    public SpatialJoinOperator(TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0,
                               TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1,
                               SpatialPredicate predicateType) {
        super(DataSetType.createDefault(keyDescriptor0.getInputType()),
                DataSetType.createDefault(keyDescriptor1.getInputType()),
                SpatialJoinOperator.createOutputDataSetType(),
                true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
        this.predicateType = predicateType;
    }

    public SpatialJoinOperator(TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0,
                               TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1,
                               DataSetType<InputType0> inputType0,
                               DataSetType<InputType1> inputType1,
                               SpatialPredicate predicateType) {
        super(inputType0, inputType1, SpatialJoinOperator.createOutputDataSetType(), true);
        this.keyDescriptor0 = keyDescriptor0;
        this.keyDescriptor1 = keyDescriptor1;
        this.predicateType = predicateType;
    }

    public SpatialJoinOperator(SpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
        this.keyDescriptor0 = that.keyDescriptor0;
        this.keyDescriptor1 = that.keyDescriptor1;
        this.predicateType = that.predicateType;
    }

    @SuppressWarnings("unchecked")
    public SpatialJoinOperator(
            FunctionDescriptor.SerializableFunction<InputType0, ? extends SpatialGeometry> keyExtractor0,
            FunctionDescriptor.SerializableFunction<InputType1, ? extends SpatialGeometry> keyExtractor1,
            Class<InputType0> input0Class,
            Class<InputType1> input1Class,
            SpatialPredicate predicateType) {
        this(
                new TransformationDescriptor<>(
                        (FunctionDescriptor.SerializableFunction<InputType0, SpatialGeometry>) keyExtractor0,
                        input0Class, SpatialGeometry.class),
                new TransformationDescriptor<>(
                        (FunctionDescriptor.SerializableFunction<InputType1, SpatialGeometry>) keyExtractor1,
                        input1Class, SpatialGeometry.class),
                predicateType
        );
    }

    public TransformationDescriptor<InputType0, ? extends SpatialGeometry> getKeyDescriptor0() {
        return this.keyDescriptor0;
    }

    public TransformationDescriptor<InputType1, ? extends SpatialGeometry> getKeyDescriptor1() {
        return this.keyDescriptor1;
    }

    public SpatialPredicate getPredicateType() {
        return this.predicateType;
    }

    private class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            return new CardinalityEstimate(10, 800, 0.9);
        }
    }
}
