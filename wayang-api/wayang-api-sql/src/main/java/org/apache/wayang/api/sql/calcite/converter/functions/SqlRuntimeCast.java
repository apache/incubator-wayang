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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;

import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.NlsString;

/**
 * Runtime SQL {@code CAST} for Wayang Java filter evaluation, delegating to
 * {@link SqlFunctions} where possible.
 */
public final class SqlRuntimeCast {

    private SqlRuntimeCast() {}

    /**
     * @param input  evaluated operand (SQL NULL is {@code null})
     * @param target destination SQL type name of the cast (from the RexCall result type)
     * @return value suitable for comparisons and filter logic
     */
    public static Object castValue(final Object input, final SqlTypeName target) {
        if (input == null) {
            return null;
        }
        final Object v = unwrapForCast(input);
        return switch (target) {
            case BOOLEAN -> SqlFunctions.toBoolean(v);
            case TINYINT -> SqlFunctions.toByte(v);
            case SMALLINT -> SqlFunctions.toShort(v);
            case INTEGER -> SqlFunctions.toInt(v);
            case BIGINT -> SqlFunctions.toLong(v);
            case DECIMAL -> SqlFunctions.toBigDecimal(v);
            case FLOAT, REAL -> castToFloat(v);
            case DOUBLE -> castToDouble(v);
            case CHAR, VARCHAR -> castToString(v);
            default -> throw new UnsupportedOperationException(
                    "CAST to " + target + " is not supported in Java filter evaluation yet.");
        };
    }

    private static Object unwrapForCast(final Object o) {
        if (o instanceof final NlsString nls) {
            return nls.getValue();
        }
        if (o instanceof final Character c) {
            return c.toString();
        }
        return o;
    }

    private static float castToFloat(final Object v) {
        if (v instanceof final DateString ds) {
            return (float) ds.getMillisSinceEpoch();
        }
        if (v instanceof final Date d) {
            return (float) d.getTime();
        }
        if (v instanceof final Calendar cal) {
            return (float) cal.getTimeInMillis();
        }
        return SqlFunctions.toFloat(v);
    }

    private static double castToDouble(final Object v) {
        if (v instanceof final DateString ds) {
            return (double) ds.getMillisSinceEpoch();
        }
        if (v instanceof final Date d) {
            return (double) d.getTime();
        }
        if (v instanceof final Calendar cal) {
            return (double) cal.getTimeInMillis();
        }
        return SqlFunctions.toDouble(v);
    }

