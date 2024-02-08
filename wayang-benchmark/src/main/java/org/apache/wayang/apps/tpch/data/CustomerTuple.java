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
 * A tuple of the customer table.
 * <p>Example line:</p>
 * <pre>
 * "1"|"Customer#001"|"Ivhasjhhd ot,c,E"|"15"|"25-989-741-2988"|"711.56"|"BUILDING"|"express instructions among the excuses nag"|
 * </pre>
 */
public class CustomerTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public int L_CUSTKEY;

    /**
     * {@code string}
     */
    public String L_NAME;

    /**
     * {@code string}
     */
    public String L_ADDRESS;

    /**
     * {@code integer}, {@code FK}
     */
    public int L_NATIONKEY;

    /**
     * {@code string}
     */
    public String L_PHONE;

    /**
     * {@code decimal}
     */
    public double L_ACCTBAL;

    /**
     * {@code string}
     */
    public String L_MKTSEGMENT;

    /**
     * {@code variable text, size 44}
     */
    public String L_COMMENT;

    public CustomerTuple(int l_CUSTKEY,
                         String l_NAME,
                         String l_ADDRESS,
                         int l_NATIONKEY,
                         String l_PHONE,
                         double l_ACCTBAL,
                         String l_MKTSEGMENT,
                         String l_COMMENT) {
        this.L_CUSTKEY = l_CUSTKEY;
        this.L_NAME = l_NAME;
        this.L_ADDRESS = l_ADDRESS;
        this.L_NATIONKEY = l_NATIONKEY;
        this.L_PHONE = l_PHONE;
        this.L_ACCTBAL = l_ACCTBAL;
        this.L_MKTSEGMENT = l_MKTSEGMENT;
        this.L_COMMENT = l_COMMENT;
    }

    public CustomerTuple() {
    }

    /**
     * Parses a {@link CustomerTuple} from a given CSV line (double quoted, comma-separated).
     */
    public static class Parser {

        public CustomerTuple parse(String line, char delimiter) {
            CustomerTuple tuple = new CustomerTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.L_CUSTKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_NAME = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_ADDRESS = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_NATIONKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_PHONE = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_ACCTBAL = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_MKTSEGMENT = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}
