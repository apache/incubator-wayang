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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.model.LinearRegressionModel;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

public class LinearRegressionOperator extends BinaryToUnaryOperator<double[], Double, LinearRegressionModel> {

    // TODO other parameters
    protected boolean fitIntercept;

    public LinearRegressionOperator(boolean fitIntercept) {
        super(DataSetType.createDefaultUnchecked(double[].class),
                DataSetType.createDefaultUnchecked(Double.class),
                DataSetType.createDefaultUnchecked(LinearRegressionModel.class),
                false);
        this.fitIntercept = fitIntercept;
    }

    public LinearRegressionOperator(LinearRegressionOperator that) {
        super(that);
        this.fitIntercept = that.fitIntercept;
    }

    public boolean getFitIntercept() {
        return fitIntercept;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        // TODO
        return super.createCardinalityEstimator(outputIndex, configuration);
    }
}
