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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.model.Model;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.TypeConverter;

import java.util.Optional;

@Deprecated
public class ModelTransformOperator<X, Y> extends BinaryToUnaryOperator<Model, X, Tuple2<X, Y>> {

    public static ModelTransformOperator<double[], Integer> kMeans() {
        // The type of TypeReference cannot be omitted, to avoid the following error.
        // error: cannot infer type arguments for TypeReference<T>, reason: cannot use '<>' with anonymous inner classes
        return new ModelTransformOperator<>(new TypeReference<double[]>() {}, new TypeReference<Tuple2<double[], Integer>>() {});
    }

    public static ModelTransformOperator<double[], Double> linearRegression() {
        return new ModelTransformOperator<>(new TypeReference<double[]>() {}, new TypeReference<Tuple2<double[], Double>>() {});
    }

    public static ModelTransformOperator<double[], Integer> decisionTreeClassification() {
        return new ModelTransformOperator<>(new TypeReference<double[]>() {}, new TypeReference<Tuple2<double[], Integer>>() {});
    }

    public ModelTransformOperator(DataSetType<X> inType, DataSetType<Tuple2<X, Y>> outType) {
        super(DataSetType.createDefaultUnchecked(Model.class), inType, outType, false);
    }

    public ModelTransformOperator(Class<X> inType, Class<Tuple2<X, Y>> outType) {
        this(DataSetType.createDefault(inType), DataSetType.createDefault(outType));
    }

    public ModelTransformOperator(TypeReference<X> inType, TypeReference<Tuple2<X, Y>> outType) {
        this(TypeConverter.convert(inType), TypeConverter.convert(outType));
    }

    public ModelTransformOperator(ModelTransformOperator<X, Y> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        // TODO
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
