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
 * A tuple of the part table.
 */
public class PartTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public long P_PARTKEY;

    /**
     * {@code string}
     */
    public String P_NAME;

    /**
     * {@code string}
     */
    public String P_MFGR;

    /**
     * {@code string}
     */
    public String P_BRAND;

    /**
     * {@code string}
     */
    public String P_TYPE;

    /**
     * {@code decimal}
     */
    public double P_SIZE;

    /**
     * {@code string}
     */
    public String P_CONTAINER;

    /**
     * {@code decimal}
     */
    public double P_RETAILPRICE;

    public String P_COMMENT;

    public PartTuple(long p_PARTKEY,
                         String p_NAME,
                         String p_MFGR,
                         String p_BRAND,
                         String p_TYPE,
                         double p_SIZE,
                         String p_CONTAINER,
                         double p_RETAILPRICE,
                         String p_COMMENT) {
        this.P_PARTKEY = p_PARTKEY;
        this.P_NAME = p_NAME;
        this.P_MFGR = p_MFGR;
        this.P_BRAND = p_BRAND;
        this.P_TYPE = p_TYPE;
        this.P_SIZE = p_SIZE;
        this.P_CONTAINER = p_CONTAINER;
        this.P_RETAILPRICE = p_RETAILPRICE;
        this.P_COMMENT = p_COMMENT;
    }

    public PartTuple() {
    }

    public static class Parser {

        public PartTuple parse(String line, char delimiter) {
            PartTuple tuple = new PartTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.P_PARTKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_NAME = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_MFGR = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_BRAND = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_TYPE = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_SIZE = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_CONTAINER = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_RETAILPRICE = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.P_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }
    }
}
