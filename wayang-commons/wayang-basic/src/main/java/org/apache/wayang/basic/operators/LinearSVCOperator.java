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


import org.apache.wayang.basic.model.SVMModel;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

public class LinearSVCOperator extends BinaryToUnaryOperator<double[], Double, SVMModel> {

    private final int maxIter;
    private final double regParam;

    public LinearSVCOperator(int maxIter, double regParam) {
        super(
                DataSetType.createDefaultUnchecked(double[].class),
                DataSetType.createDefaultUnchecked(Double.class),
                DataSetType.createDefaultUnchecked(SVMModel.class),
                false
        );
        this.maxIter = maxIter;
        this.regParam = regParam;
    }

    public LinearSVCOperator(LinearSVCOperator that) {
        super(that);
        this.maxIter = that.maxIter;
        this.regParam = that.regParam;
    }

    public int getMaxIter() {
        return maxIter;
    }

    public double getRegParam() {
        return regParam;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
