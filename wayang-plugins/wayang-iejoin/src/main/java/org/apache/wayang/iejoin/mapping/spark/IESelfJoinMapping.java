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

package org.apache.wayang.iejoin.mapping.spark;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.mapping.OperatorPattern;
import org.apache.wayang.core.mapping.PlanTransformation;
import org.apache.wayang.core.mapping.ReplacementSubplanFactory;
import org.apache.wayang.core.mapping.SubplanMatch;
import org.apache.wayang.core.mapping.SubplanPattern;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.iejoin.operators.IEJoinMasterOperator;
import org.apache.wayang.iejoin.operators.IESelfJoinOperator;
import org.apache.wayang.iejoin.operators.SparkIESelfJoinOperator;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * Mapping from {@link IESelfJoinOperator} to {@link SparkIESelfJoinOperator}.
 */
public class IESelfJoinMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(new PlanTransformation(this.createSubplanPattern(), new ReplacementFactory(),
                SparkPlatform.getInstance()));
    }

    private SubplanPattern createSubplanPattern() {
        final OperatorPattern operatorPattern = new OperatorPattern(
                "ieselfjoin", new IESelfJoinOperator<>(DataSetType.none(), null, IEJoinMasterOperator.JoinCondition.GreaterThan, null, IEJoinMasterOperator.JoinCondition.GreaterThan), false);
        return SubplanPattern.createSingleton(operatorPattern);
    }

    private static class ReplacementFactory<InputType0 extends Record, InputType1 extends Record,
            Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>> extends ReplacementSubplanFactory {

        @Override
        protected Operator translate(SubplanMatch subplanMatch, int epoch) {
            final IESelfJoinOperator<?, ?, ?> originalOperator = (IESelfJoinOperator<?, ?, ?>) subplanMatch.getMatch("ieselfjoin").getOperator();
            return new SparkIESelfJoinOperator(originalOperator.getInputType(), originalOperator.getGet0Pivot(), originalOperator.getCond0(), originalOperator.getGet0Ref(), originalOperator.getCond1()).at(epoch);
        }
    }
}
