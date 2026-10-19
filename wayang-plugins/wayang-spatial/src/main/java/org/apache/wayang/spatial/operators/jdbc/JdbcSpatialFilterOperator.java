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

import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.spatial.function.JtsSpatialPredicate;
import org.apache.wayang.jdbc.compiler.FunctionCompiler;
import org.apache.wayang.jdbc.operators.JdbcExecutionOperator;

import java.sql.Connection;


/**
 * Template for JDBC-based {@link SpatialFilterOperator}.
 */
public abstract class JdbcSpatialFilterOperator<Type> extends SpatialFilterOperator<Type> implements JdbcExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public JdbcSpatialFilterOperator(SpatialPredicate relation,
                                     FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                     DataSetType<Type> inputClassDatasetType,
                                     SpatialGeometry geometry) {
        super(relation, keyExtractor, inputClassDatasetType, geometry);
    }

    public JdbcSpatialFilterOperator(SpatialFilterOperator<Type> that) {
        super(that);
    }

    @Override
    public String createSqlClause(Connection connection, FunctionCompiler compiler) {
        if (this.referenceGeometry == null) {
            throw new IllegalStateException("Geometry for spatial filter must not be null.");
        }

        // Column expression (e.g. "geom" or "t.geom")
        final String columnExpr = this.keyDescriptor.getSqlImplementation().getField1();

        // Geometry literal as ST_GeomFromText('WKT', srid)
        final String wkt = this.referenceGeometry.toWKT();
        // TODO: Check which SRID to use.
        final int srid = 4326;

        final String geomLiteral;
        if (srid > 0) {
            geomLiteral = String.format("ST_GeomFromText('%s', %d)", wkt, srid);
        } else {
            geomLiteral = String.format("ST_GeomFromText('%s')", wkt);
        }

        JtsSpatialPredicate relation = JtsSpatialPredicate.of(this.predicateType);
        return relation.toSql(columnExpr, geomLiteral);
    }
}
