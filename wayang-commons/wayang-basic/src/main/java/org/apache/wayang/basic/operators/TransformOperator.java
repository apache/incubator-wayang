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

public class TransformOperator<IN, OUT> extends BinaryToUnaryOperator<Model<IN, OUT>, IN, OUT> {

    public static TransformOperator<double[], Tuple2<double[], Integer>> kMeans() {
        return new TransformOperator<>(new TypeReference<>() {}, new TypeReference<>() {});
    }

    public TransformOperator(DataSetType<IN> inType, DataSetType<OUT> outType) {
        // TODO createDefaultUnchecked or createDefault?
        super(DataSetType.createDefaultUnchecked(Model.class), inType, outType, false);
    }

    public TransformOperator(Class<IN> inType, Class<OUT> outType) {
        this(DataSetType.createDefault(inType), DataSetType.createDefault(outType));
    }

    public TransformOperator(TypeReference<IN> inType, TypeReference<OUT> outType) {
        this(TypeConverter.convert(inType), TypeConverter.convert(outType));
    }

    public TransformOperator(TransformOperator<IN, OUT> that) {
        super(that);
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        // TODO
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
