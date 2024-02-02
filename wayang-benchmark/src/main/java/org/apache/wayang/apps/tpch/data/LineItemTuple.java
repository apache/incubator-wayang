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
 * A tuple of the lineitem table.
 * <p>Example line:</p>
 * <pre>
 * "3249925";"37271";"9775";"1";"9.00";"10874.43";"0.10";"0.04";"N";"O";"1998-04-19";"1998-06-17";"1998-04-21";"TAKE BACK RETURN         ";"AIR       ";"express instructions among the excuses nag"
 * </pre>
 */
public class LineItemTuple implements Serializable {

    /**
     * {@code identifier}, {@code PK}
     */
    public long L_ORDERKEY;

    /**
     * {@code identifier}
     */
    public long L_PARTKEY;

    /**
     * {@code identifier}
     */
    public long L_SUPPKEY;

    /**
     * {@code integer}, {@code PK}
     */
    public int L_LINENUMBER;

    /**
     * {@code decimal}
     */
    public double L_QUANTITY;

    /**
     * {@code decimal}
     */
    public double L_EXTENDEDPRICE;

    /**
     * {@code decimal}
     */
    public double L_DISCOUNT;

    /**
     * {@code decimal}
     */
    public double L_TAX;

    /**
     * {@code fixed text, size 1}
     */
    public char L_RETURNFLAG;

    /**
     * {@code fixed text, size 1}
     */
    public char L_LINESTATUS;

    /**
     * {@code fixed text, size 1}
     */
    public int L_SHIPDATE;

    /**
     * {@code fixed text, size 1}
     */
    public int L_COMMITDATE;

    /**
     * {@code fixed text, size 1}
     */
    public int L_RECEIPTDATE;

    /**
     * {@code fixed text, size 25}
     */
    public String L_SHIPINSTRUCT;

    /**
     * {@code fixed text, size 10}
     */
    public String L_SHIPMODE;

    /**
     * {@code variable text, size 44}
     */
    public String L_COMMENT;

    public LineItemTuple(long l_ORDERKEY,
                         long l_PARTKEY,
                         long l_SUPPKEY,
                         int l_LINENUMBER,
                         double l_QUANTITY,
                         double l_EXTENDEDPRICE,
                         double l_DISCOUNT,
                         double l_TAX,
                         char l_RETURNFLAG,
                         int l_SHIPDATE,
                         int l_COMMITDATE,
                         int l_RECEIPTDATE,
                         String l_SHIPINSTRUCT,
                         String l_SHIPMODE,
                         String l_COMMENT) {
        this.L_ORDERKEY = l_ORDERKEY;
        this.L_PARTKEY = l_PARTKEY;
        this.L_SUPPKEY = l_SUPPKEY;
        this.L_LINENUMBER = l_LINENUMBER;
        this.L_QUANTITY = l_QUANTITY;
        this.L_EXTENDEDPRICE = l_EXTENDEDPRICE;
        this.L_DISCOUNT = l_DISCOUNT;
        this.L_TAX = l_TAX;
        this.L_RETURNFLAG = l_RETURNFLAG;
        this.L_SHIPDATE = l_SHIPDATE;
        this.L_COMMITDATE = l_COMMITDATE;
        this.L_RECEIPTDATE = l_RECEIPTDATE;
        this.L_SHIPINSTRUCT = l_SHIPINSTRUCT;
        this.L_SHIPMODE = l_SHIPMODE;
        this.L_COMMENT = l_COMMENT;
    }

    public LineItemTuple() {
    }

    /**
     * Parses a {@link LineItemTuple} from a given CSV line (double quoted, comma-separated).
     */
    public static class Parser {

        public LineItemTuple parse(String line, char delimiter) {
            LineItemTuple tuple = new LineItemTuple();

            int startPos = 0;
            int endPos = line.indexOf(delimiter, startPos);
            tuple.L_ORDERKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_PARTKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_SUPPKEY = Long.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_LINENUMBER = Integer.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_QUANTITY = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_EXTENDEDPRICE = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_DISCOUNT = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_TAX = Double.valueOf(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_RETURNFLAG = line.charAt(startPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_LINESTATUS = line.charAt(startPos);

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_SHIPDATE = parseDate(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_COMMITDATE = parseDate(line.substring(startPos, endPos));

            startPos = endPos + 1;
            endPos = line.indexOf(delimiter, startPos);
            tuple.L_RECEIPTDATE = parseDate(line.substring(startPos, endPos));

            startPos = endPos + 1;
            //endPos = startPos;
            endPos = line.indexOf(delimiter, startPos);
            /*
            do {
                endPos++;
                endPos = line.indexOf(delimiter, endPos);
            } while (line.charAt(endPos) != '"' || line.charAt(endPos + 1) != '"');*/
            tuple.L_SHIPINSTRUCT = line.substring(startPos, endPos);

            startPos = endPos + 1;
            //endPos = startPos;
            endPos = line.indexOf(delimiter, startPos);
            /*
            do {
                endPos++;
                endPos = line.indexOf(delimiter, endPos);
            } while (line.charAt(endPos) != '"' || line.charAt(endPos + 1) != '"');*/
            tuple.L_SHIPMODE = line.substring(startPos, endPos);

            startPos = endPos + 1;
            //endPos = startPos;
            endPos = line.indexOf(delimiter, startPos);
            /*
            do {
                endPos++;
                endPos = line.indexOf(delimiter, endPos);
            } while (endPos >= 0 && (line.charAt(endPos) != '"' || (endPos < line.length() - 1 && line.charAt(endPos + 1) != '"')));*/
            assert endPos < 0 : String.format("Parsing error: unexpected ';' at %d. Input: %s", endPos, line);
            endPos = line.length();
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
