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

package org.apache.wayang.spatial.operators.postgres;

import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spatial.operators.jdbc.JdbcSpatialFilterOperator;
import org.apache.wayang.postgres.operators.PostgresExecutionOperator;


/**
 * PostgreSQL implementation of the {@link SpatialFilterOperator}.
 */
public class PostgresSpatialFilterOperator<Type> extends JdbcSpatialFilterOperator<Type> implements PostgresExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public PostgresSpatialFilterOperator(SpatialPredicate relation,
                                         FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                         DataSetType<Type> inputClassDatasetType,
                                         SpatialGeometry geometry) {
        super(relation, keyExtractor, inputClassDatasetType, geometry);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresSpatialFilterOperator(SpatialFilterOperator<Type> that) {
        super(that);
    }

    @Override
    protected PostgresSpatialFilterOperator<Type> createCopy() {
        return new PostgresSpatialFilterOperator<>(this);
    }
}
