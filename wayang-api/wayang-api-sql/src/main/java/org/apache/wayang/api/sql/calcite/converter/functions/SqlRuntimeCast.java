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
        switch (target) {
            case BOOLEAN:
                return SqlFunctions.toBoolean(v);
            case TINYINT:
                return SqlFunctions.toByte(v);
            case SMALLINT:
                return SqlFunctions.toShort(v);
            case INTEGER:
                return SqlFunctions.toInt(v);
            case BIGINT:
                return SqlFunctions.toLong(v);
            case DECIMAL:
                return SqlFunctions.toBigDecimal(v);
            case FLOAT:
            case REAL:
                return castToFloat(v);
            case DOUBLE:
                return castToDouble(v);
            case CHAR:
            case VARCHAR:
                return castToString(v);
            default:
                throw new UnsupportedOperationException(
                        "CAST to " + target + " is not supported in Java filter evaluation yet.");
        }
    }

    private static Object unwrapForCast(final Object o) {
        if (o instanceof NlsString) {
            return ((NlsString) o).getValue();
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

    private static String castToString(final Object v) {
        if (v instanceof final String s) {
            return s;
        }
        if (v instanceof final NlsString nls) {
            return nls.getValue();
        }
        if (v instanceof final Boolean b) {
            return SqlFunctions.toString(b);
        }
        if (v instanceof final Float f) {
            return SqlFunctions.toString(f);
        }
        if (v instanceof final Double d) {
            return SqlFunctions.toString(d);
        }
        if (v instanceof final BigDecimal bd) {
            return SqlFunctions.toString(bd);
        }
        if (v instanceof final Number n) {
            return n.toString();
        }
        if (v instanceof final DateString ds) {
            return ds.toString();
        }
        if (v instanceof final Character c) {
            return c.toString();
        }
        if (v instanceof final Date d) {
            return d.toString();
        }
        if (v instanceof final Calendar cal) {
            return cal.getTime().toString();
        }
        return String.valueOf(v);
    }
}
