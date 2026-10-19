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

import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.spatial.operators.jdbc.JdbcSpatialJoinOperator;
import org.apache.wayang.postgres.operators.PostgresExecutionOperator;

public class PostgresSpatialJoinOperator<InputType0, InputType1> extends JdbcSpatialJoinOperator<InputType0, InputType1> implements PostgresExecutionOperator {
    /**
     * Creates a new instance.
     *
     * @param predicate the type of spatial join (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public PostgresSpatialJoinOperator(TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0,
                                       TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1,
                                       SpatialPredicate predicate) {
        super(keyDescriptor0, keyDescriptor1, predicate);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PostgresSpatialJoinOperator(SpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    protected PostgresSpatialJoinOperator<InputType0, InputType1> createCopy() {
        return new PostgresSpatialJoinOperator<>(this);
    }
}
