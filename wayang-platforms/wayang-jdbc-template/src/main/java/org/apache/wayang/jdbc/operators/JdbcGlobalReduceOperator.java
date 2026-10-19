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

import java.sql.Connection;
import java.util.Optional;

import org.apache.wayang.basic.operators.GlobalReduceOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;

public abstract class JdbcGlobalReduceOperator extends GlobalReduceOperator<Record>
        implements JdbcExecutionOperator {

    public JdbcGlobalReduceOperator(final GlobalReduceOperator<Record> globalReduceOperator) {
        super(globalReduceOperator);
    }

    public JdbcGlobalReduceOperator(final ReduceDescriptor<Record> reduceDescriptor) {
        super(reduceDescriptor, DataSetType.createDefault(Record.class));
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.globalreduce.load", this.getPlatform().getPlatformId());
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        return reduceDescriptor.getSqlImplementation();
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator = JdbcExecutionOperator.super.createLoadProfileEstimator(
                configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.reduceDescriptor, configuration);
        return optEstimator;
    }
}
