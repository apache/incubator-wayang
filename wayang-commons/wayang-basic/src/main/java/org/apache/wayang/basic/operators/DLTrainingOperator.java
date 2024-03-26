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

import org.apache.wayang.basic.model.DLModel;
import org.apache.wayang.basic.model.op.Op;
import org.apache.wayang.basic.model.optimizer.Optimizer;
import org.apache.wayang.core.plan.wayangplan.BinaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

public class DLTrainingOperator<X, Y> extends BinaryToUnaryOperator<X, Y, DLModel>  {

    protected DLModel model;
    protected Option option;

    public DLTrainingOperator(DLModel model, Option option, Class<X> xType, Class<Y> yType) {
        super(DataSetType.createDefault(xType),
                DataSetType.createDefault(yType),
                DataSetType.createDefault(DLModel.class),
                false);
        this.model = model;
        this.option = option;
    }

    public DLTrainingOperator(DLModel model, Option option, DataSetType<X> xType, DataSetType<Y> yType) {
        super(xType, yType, DataSetType.createDefault(DLModel.class), false);
        this.model = model;
        this.option = option;
    }

    public DLTrainingOperator(DLTrainingOperator<X, Y> that) {
        super(that);
        this.model = that.model;
        this.option = that.option;
    }

    public static class Option {
        private final Op criterion;
        private final Optimizer optimizer;
        private final int batchSize;
        private final int epoch;

        private Op accuracyCalculation;

        public Option(Op criterion, Optimizer optimizer, int batchSize, int epoch) {
            this.criterion = criterion;
            this.batchSize = batchSize;
            this.optimizer = optimizer;
            this.epoch = epoch;
        }

        public Op getCriterion() {
            return criterion;
        }

        public Optimizer getOptimizer() {
            return optimizer;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public int getEpoch() {
            return epoch;
        }

        public void setAccuracyCalculation(Op accuracyCalculation) {
            this.accuracyCalculation = accuracyCalculation;
        }

        public Op getAccuracyCalculation() {
            return accuracyCalculation;
        }
    }
}
