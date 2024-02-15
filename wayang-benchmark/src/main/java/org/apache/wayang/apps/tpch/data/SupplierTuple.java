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
 * A tuple of the supplier table.
 */
public class SupplierTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public long S_SUPPKEY;

    /**
     * {@code string}
     */
    public String S_NAME;

    /**
     * {@code string}
     */
    public String S_ADDRESS;

    /**
     * {@code integer}, {@code FK}
     */
    public int S_NATIONKEY;

    /**
     * {@code string}
     */
    public String S_PHONE;

    /**
     * {@code decimal}
     */
    public double S_ACCTBAL;

    public String S_COMMENT;

    public SupplierTuple(long s_SUPPKEY,
                         String s_NAME,
                         String s_ADDRESS,
                         int s_NATIONKEY,
                         String s_PHONE,
                         double s_ACCTBAL,
                         String s_COMMENT) {
        this.S_SUPPKEY = s_SUPPKEY;
        this.S_NAME = s_NAME;
        this.S_ADDRESS = s_ADDRESS;
        this.S_NATIONKEY = s_NATIONKEY;
        this.S_PHONE = s_PHONE;
        this.S_ACCTBAL = s_ACCTBAL;
        this.S_COMMENT = s_COMMENT;
    }

    public SupplierTuple() {
    }

    public static class Parser {

        public SupplierTuple parse(String line, char delimiter) {
            SupplierTuple tuple = new SupplierTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.S_SUPPKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_NAME = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_ADDRESS = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_NATIONKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_PHONE = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_ACCTBAL = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.S_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}
