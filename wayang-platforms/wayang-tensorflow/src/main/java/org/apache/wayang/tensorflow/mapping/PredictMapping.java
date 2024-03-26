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

package org.apache.wayang.tensorflow.mapping;

import org.apache.wayang.basic.operators.PredictOperator;
import org.apache.wayang.core.mapping.*;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.tensorflow.operators.TensorflowPredictOperator;
import org.apache.wayang.tensorflow.platform.TensorflowPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link PredictOperator} to {@link TensorflowPredictOperator}.
 */
public class PredictMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(
                this.createSubplanPattern(),
                this.createReplacementSubplanFactory(),
                TensorflowPlatform.getInstance()
        ));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "predict", new PredictOperator(DataSetType.none(), DataSetType.none()), false
        );
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private ReplacementSubplanFactory createReplacementSubplanFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<PredictOperator>(
                (matchedOperator, epoch) -> new TensorflowPredictOperator(matchedOperator).at(epoch)
        );
    }
}
