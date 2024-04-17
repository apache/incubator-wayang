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

package org.apache.wayang.apps.tpch.data;

import java.io.Serializable;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * A tuple of the partsupplier table.
 */
public class PartSupplierTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public long PS_PARTKEY;


    public long PS_SUPPLKEY;

    /**
     * {@code decimal}
     */
    public double PS_AVAILQTY;

    public double PS_SUPPLYCOST;

    public String PS_COMMENT;

    public PartSupplierTuple(long ps_PARTKEY,
                         long ps_SUPPLKEY,
                         double ps_AVAILQTY,
                         double ps_SUPPLYCOST,
                         String ps_COMMENT) {
        this.PS_PARTKEY = ps_PARTKEY;
        this.PS_SUPPLKEY = ps_SUPPLKEY;
        this.PS_AVAILQTY = ps_AVAILQTY;
        this.PS_SUPPLYCOST = ps_SUPPLYCOST;
        this.PS_COMMENT = ps_COMMENT;
    }

    public PartSupplierTuple() {
    }

    public static class Parser {

        public PartSupplierTuple parse(String line, char delimiter) {
            PartSupplierTuple tuple = new PartSupplierTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.PS_PARTKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.PS_SUPPLKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.PS_AVAILQTY = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.PS_SUPPLYCOST = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.PS_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}
