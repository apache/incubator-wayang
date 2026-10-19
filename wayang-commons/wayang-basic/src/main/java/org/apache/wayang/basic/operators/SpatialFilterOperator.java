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

import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;


/**
 * This operator returns a new dataset after filtering by applying a spatial predicate.
 */
public class SpatialFilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    protected final SpatialPredicate predicateType;
    protected final TransformationDescriptor<Type, SpatialGeometry> keyDescriptor;
    protected final SpatialGeometry referenceGeometry;

    @SuppressWarnings("unchecked")
    public SpatialFilterOperator(SpatialPredicate predicateType,
                                 FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                 DataSetType<Type> inputClassDatasetType,
                                 SpatialGeometry geometry) {
        super(inputClassDatasetType, inputClassDatasetType, true);
        this.predicateType = predicateType;
        this.keyDescriptor = new TransformationDescriptor<>(
                (FunctionDescriptor.SerializableFunction<Type, SpatialGeometry>) (FunctionDescriptor.SerializableFunction) keyExtractor,
                inputClassDatasetType.getDataUnitType().getTypeClass(), SpatialGeometry.class);
        this.referenceGeometry = geometry;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SpatialFilterOperator(SpatialFilterOperator<Type> that) {
        super(that);
        this.predicateType = that.predicateType;
        this.keyDescriptor = that.keyDescriptor;
        this.referenceGeometry = that.referenceGeometry;
    }

    public SpatialPredicate getPredicateType() {
        return this.predicateType;
    }

    public SpatialGeometry getReferenceGeometry() {
        return this.referenceGeometry;
    }

    public TransformationDescriptor<Type, SpatialGeometry> getKeyDescriptor() {
        return this.keyDescriptor;
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link SpatialFilterOperator}s.
     */
    private class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            return new CardinalityEstimate(10, 800, 0.9);
        }
    }
}
