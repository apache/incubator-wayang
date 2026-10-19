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

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.SortOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;


public abstract class JdbcSortOperator extends SortOperator<Record, Record> implements JdbcExecutionOperator {
    public JdbcSortOperator(final TransformationDescriptor<Record, Record> keyDescriptor) {
        super(keyDescriptor);
    }

    public JdbcSortOperator(final SortOperator<Record, Record> that) {
        super(that);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return String.format("wayang.%s.sort.load", this.getPlatform().getPlatformId());
    }

    @Override
    public String createSqlClause(final Connection connection, final FunctionCompiler compiler) {
        return " ORDER BY " + keyDescriptor.getSqlImplementation().field0 + " " + keyDescriptor.getSqlImplementation().field1;
    }

    @Override
    public Optional<LoadProfileEstimator> createLoadProfileEstimator(final Configuration configuration) {
        final Optional<LoadProfileEstimator> optEstimator =
                JdbcExecutionOperator.super.createLoadProfileEstimator(configuration);
        LoadProfileEstimators.nestUdfEstimator(optEstimator, this.getKeyDescriptor(), configuration);
        return optEstimator;
    }
}
