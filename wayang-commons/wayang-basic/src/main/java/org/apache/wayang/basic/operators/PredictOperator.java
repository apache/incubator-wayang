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
import org.apache.wayang.basic.model.Model;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.TypeConverter;

public class PredictOperator<X, Y> extends BinaryToUnaryOperator<Model, X, Y> {

    public PredictOperator(DataSetType<X> inType, DataSetType<Y> outType) {
        super(DataSetType.createDefaultUnchecked(Model.class), inType, outType, false);
    }

    public PredictOperator(Class<X> inType, Class<Y> outType) {
        this(DataSetType.createDefault(inType), DataSetType.createDefault(outType));
    }

    public PredictOperator(TypeReference<X> inType, TypeReference<Y> outType) {
        this(TypeConverter.convert(inType), TypeConverter.convert(outType));
    }

    public PredictOperator(PredictOperator<X, Y> that) {
        super(that);
    }
}
