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

package org.apache.wayang.spatial.function;

import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.function.BiPredicate;

public enum JtsSpatialPredicate {

    INTERSECTS("INTERSECTS", "ST_Intersects", Geometry::intersects),
    CONTAINS("CONTAINS", "ST_Contains", Geometry::contains),
    WITHIN("WITHIN", "ST_Within", Geometry::within),
    TOUCHES("TOUCHES", "ST_Touches", Geometry::touches),
    OVERLAPS("OVERLAPS", "ST_Overlaps", Geometry::overlaps),
    CROSSES("CROSSES", "ST_Crosses", Geometry::crosses),
    EQUALS("EQUALS", "ST_Equals", Geometry::equalsTopo);

    private final String opName;
    private final String sqlFunctionName;
    private final BiPredicate<Geometry, Geometry> predicate;

    JtsSpatialPredicate(String opName,
                        String sqlFunctionName,
                        BiPredicate<Geometry, Geometry> predicate) {
        this.opName = opName;
        this.sqlFunctionName = sqlFunctionName;
        this.predicate = predicate;
    }

    public static JtsSpatialPredicate fromString(String opName) {
        return Arrays.stream(values())
                .filter(r -> r.opName.equalsIgnoreCase(opName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported spatial filter type: " + opName));
    }

    /**
     * Convert from the core module's {@link SpatialPredicate} to this enum.
     *
     * @param predicate the spatial predicate
     * @return the corresponding JtsSpatialPredicate
     */
    public static JtsSpatialPredicate of(SpatialPredicate predicate) {
        return switch (predicate) {
            case INTERSECTS -> INTERSECTS;
            case CONTAINS -> CONTAINS;
            case WITHIN -> WITHIN;
            case OVERLAPS -> OVERLAPS;
            case TOUCHES -> TOUCHES;
            case CROSSES -> CROSSES;
            case EQUALS -> EQUALS;
        };
    }

    public boolean test(Geometry candidate, Geometry reference) {
        return predicate.test(candidate, reference);
    }

    public String toSql(String columnExpr, String geomLiteral) {
        return String.format("%s(%s, %s)", this.sqlFunctionName, columnExpr, geomLiteral);
    }

    public String toSql(String leftTable, String leftKey, String rightTable, String rightKey) {
        return String.format("%s(%s.%s, %s.%s)", this.sqlFunctionName, leftTable, leftKey, rightTable, rightKey);
    }
}
