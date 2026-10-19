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

package org.apache.wayang.jdbc.operators;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

import java.sql.Connection;
import java.util.Optional;

public abstract class JdbcReduceByOperator extends ReduceByOperator<Record, Record> implements JdbcExecutionOperator {
    public JdbcReduceByOperator(final TransformationDescriptor<Record, Record> keyDescriptor,
            final ReduceDescriptor<Record> reduceDescriptor) {
        super(keyDescriptor, reduceDescriptor, DataSetType.createDefault(Record.class));
    }

    public JdbcReduceByOperator(final ReduceByOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.reduceby.load", this.getPlatform().getPlatformId());
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        return keyDescriptor.getSqlImplementation().getField0() + "," + reduceDescriptor.getSqlImplementation();
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = JdbcExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
        return optEstimator;
    }
}
