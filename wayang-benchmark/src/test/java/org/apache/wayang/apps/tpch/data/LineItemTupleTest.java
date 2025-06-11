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

import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suited for {@link LineItemTuple}.
 */
class LineItemTupleTest {

    @Test
    void testParser() {
        LineItemTuple.Parser parser = new LineItemTuple.Parser();
        final LineItemTuple tuple = parser.parse("\"3249925\";\"37271\";\"9775\";\"1\";\"9.00\";\"10874.43\";\"0.10\";" +
                "\"0.04\";\"N\";\"O\";\"1998-04-19\";\"1998-06-17\";\"1998-04-21\";\"TAKE BACK RETURN         \";" +
                "\"AIR       \";\"express instructions among the excuses nag\"");

        assertEquals(3249925, tuple.L_ORDERKEY);
        assertEquals(37271, tuple.L_PARTKEY);
        assertEquals(9775, tuple.L_SUPPKEY);
        assertEquals(1, tuple.L_LINENUMBER);
        assertEquals(9.00, tuple.L_QUANTITY, 0);
        assertEquals(10874.43, tuple.L_EXTENDEDPRICE, 0.001);
        assertEquals(0.10, tuple.L_DISCOUNT, 0.001);
        assertEquals(0.04, tuple.L_TAX, 0.001);
        assertEquals('N', tuple.L_RETURNFLAG);
        assertEquals('O', tuple.L_LINESTATUS);
        assertEquals(this.toDateInteger(1998, 4, 19), tuple.L_SHIPDATE);
        assertEquals(this.toDateInteger(1998, 6, 17), tuple.L_COMMITDATE);
        assertEquals(this.toDateInteger(1998, 4, 21), tuple.L_RECEIPTDATE);
        assertEquals("TAKE BACK RETURN         ", tuple.L_SHIPINSTRUCT);
        assertEquals("AIR       ", tuple.L_SHIPMODE);
        assertEquals("express instructions among the excuses nag", tuple.L_COMMENT);
    }

    private int toDateInteger(int year, int month, int date) {
        final int[] months =new int[]{
                Calendar.JANUARY, Calendar.FEBRUARY, Calendar.MARCH, Calendar.APRIL,
                Calendar.MAY, Calendar.JUNE, Calendar.JULY, Calendar.AUGUST,
                Calendar.SEPTEMBER, Calendar.OCTOBER, Calendar.NOVEMBER, Calendar.DECEMBER
        };
        Calendar calendar = GregorianCalendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, months[month - 1]);
        calendar.set(Calendar.DAY_OF_MONTH, date);
        return (int) (calendar.getTimeInMillis() / (1000 * 60 * 60 * 24));
    }

}
