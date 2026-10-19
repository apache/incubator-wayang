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

package org.apache.wayang.spatial.operators.jdbc;

import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spatial.function.JtsSpatialPredicate;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;

import java.sql.Connection;

public abstract class JdbcSpatialJoinOperator<InputType0, InputType1>
        extends SpatialJoinOperator<InputType0, InputType1>
        implements JdbcExecutionOperator {


    public JdbcSpatialJoinOperator(
            TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0,
            TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1,
            SpatialPredicate predicateType
    ) {
        super(
                keyDescriptor0,
                keyDescriptor1,
                predicateType
        );
    }

    /**
     * Copies an instance.
     *
     * @param that that should be copied
     */
    public JdbcSpatialJoinOperator(SpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }

    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        final Tuple<String, String> left = this.keyDescriptor0.getSqlImplementation();
        final Tuple<String, String> right = this.keyDescriptor1.getSqlImplementation();
        if (left == null || right == null) {
            throw new IllegalStateException("Spatial join requires SQL implementations for both inputs.");
        }
        final String leftTableName = left.field0;
        final String leftKey = left.field1;
        final String rightTableName = right.field0;
        final String rightKey = right.field1;

        JtsSpatialPredicate predicate = JtsSpatialPredicate.of(this.predicateType);
        return "JOIN " + rightTableName + " ON " +
                predicate.toSql(leftTableName, leftKey, rightTableName, rightKey);
    }
}
