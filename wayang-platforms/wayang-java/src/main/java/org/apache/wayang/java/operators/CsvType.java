/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.java.operators;

import java.io.Serializable;

/** CSV field type, including optional decimal precision and scale, without SQL API dependencies. */
public final class CsvType implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Kind {
        BOOLEAN, TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE, DECIMAL, DATE, TIME, TIMESTAMP, STRING
    }

    public static final CsvType BOOLEAN = new CsvType(Kind.BOOLEAN, -1, -1);
    public static final CsvType TINYINT = new CsvType(Kind.TINYINT, -1, -1);
    public static final CsvType SMALLINT = new CsvType(Kind.SMALLINT, -1, -1);
    public static final CsvType INTEGER = new CsvType(Kind.INTEGER, -1, -1);
    public static final CsvType BIGINT = new CsvType(Kind.BIGINT, -1, -1);
    public static final CsvType FLOAT = new CsvType(Kind.FLOAT, -1, -1);
    public static final CsvType DOUBLE = new CsvType(Kind.DOUBLE, -1, -1);
    public static final CsvType DECIMAL = new CsvType(Kind.DECIMAL, -1, -1);
    public static final CsvType DATE = new CsvType(Kind.DATE, -1, -1);
    public static final CsvType TIME = new CsvType(Kind.TIME, -1, -1);
    public static final CsvType TIMESTAMP = new CsvType(Kind.TIMESTAMP, -1, -1);
    public static final CsvType STRING = new CsvType(Kind.STRING, -1, -1);

    private final Kind kind;
    private final int precision;
    private final int scale;

    private CsvType(Kind kind, int precision, int scale) {
        this.kind = kind;
        this.precision = precision;
        this.scale = scale;
    }

    /** Creates a decimal type with the precision and scale declared by the source schema. */
    public static CsvType decimal(int precision, int scale) {
        return new CsvType(Kind.DECIMAL, precision, scale);
    }

    public Kind getKind() {
        return this.kind;
    }

    public int getPrecision() {
        return this.precision;
    }

    public int getScale() {
        return this.scale;
    }
}
