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
 * A tuple of the order table.
 */
public class OrderTuple implements Serializable {

    /**
     * {@code integer}, {@code PK}
     */
    public int L_ORDERKEY;

    /**
     * {@code integer}, {@code FK}
     */
    public int L_CUSTKEY;

    /**
     * {@code string}
     */
    public String L_ORDERSTATUS;

    /**
     * {@code decimal}
     */
    public double L_TOTALPRICE;

    /**
     * {@code fixed text, size 1}
     */
    public int L_ORDERDATE;

    /**
     * {@code string}
     */
    public String L_ORDERPRIORITY;

    /**
     * {@code string}
     */
    public String L_CLERK;

    /**
     * {@code string}
     */
    public int L_SHIPPRIORITY;

    /**
     * {@code variable text, size 44}
     */
    public String L_COMMENT;

    public OrderTuple(int l_ORDERKEY,
                         int l_CUSTKEY,
                         String l_ORDERSTATUS,
                         double l_TOTALPRICE,
                         int l_ORDERDATE,
                         String l_ORDERPRIORITY,
                         String l_CLERK,
                         int l_SHIPPRIORITY,
                         String l_COMMENT) {
        this.L_ORDERKEY = l_ORDERKEY;
        this.L_CUSTKEY = l_CUSTKEY;
        this.L_ORDERSTATUS = l_ORDERSTATUS;
        this.L_TOTALPRICE = l_TOTALPRICE;
        this.L_ORDERDATE = l_ORDERDATE;
        this.L_ORDERPRIORITY = l_ORDERPRIORITY;
        this.L_CLERK = l_CLERK;
        this.L_SHIPPRIORITY = l_SHIPPRIORITY;
        this.L_COMMENT = l_COMMENT;
    }

    public OrderTuple() {
    }

    /**
     * Parses a {@link OrderTuple} from a given CSV line (double quoted, comma-separated).
     */
    public static class Parser {

        public OrderTuple parse(String line, char delimiter) {
            OrderTuple tuple = new OrderTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.L_ORDERKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_CUSTKEY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_ORDERSTATUS = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_TOTALPRICE = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_ORDERDATE = parseDate(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_ORDERPRIORITY = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_CLERK = line.substring(startPos, endPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_SHIPPRIORITY = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_COMMENT = line.substring(startPos, endPos);

            return tuple;
        }

        public static int parseDate(String dateString) {
            Calendar calendar = GregorianCalendar.getInstance();
            calendar.set(
                    Integer.parseInt(dateString.substring(0, 4)),
                    Integer.parseInt(dateString.substring(5, 7)) - 1,
                    Integer.parseInt(dateString.substring(8, 10))
            );
            final int millisPerDay = 1000 * 60 * 60 * 24;
            return (int) (calendar.getTimeInMillis() / millisPerDay);
        }
    }
}
