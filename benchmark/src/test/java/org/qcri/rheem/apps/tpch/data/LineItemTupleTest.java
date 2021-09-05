package org.qcri.rheem.apps.tpch.data;

import org.junit.Assert;
import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Test suited for {@link LineItemTuple}.
 */
public class LineItemTupleTest {

    @Test
    public void testParser() {
        LineItemTuple.Parser parser = new LineItemTuple.Parser();
        final LineItemTuple tuple = parser.parse("\"3249925\";\"37271\";\"9775\";\"1\";\"9.00\";\"10874.43\";\"0.10\";" +
                "\"0.04\";\"N\";\"O\";\"1998-04-19\";\"1998-06-17\";\"1998-04-21\";\"TAKE BACK RETURN         \";" +
                "\"AIR       \";\"express instructions among the excuses nag\"");

        Assert.assertEquals(3249925, tuple.L_ORDERKEY);
        Assert.assertEquals(37271, tuple.L_PARTKEY);
        Assert.assertEquals(9775, tuple.L_SUPPKEY);
        Assert.assertEquals(1, tuple.L_LINENUMBER);
        Assert.assertEquals(9.00, tuple.L_QUANTITY, 0);
        Assert.assertEquals(10874.43, tuple.L_EXTENDEDPRICE, 0.001);
        Assert.assertEquals(0.10, tuple.L_DISCOUNT, 0.001);
        Assert.assertEquals(0.04, tuple.L_TAX, 0.001);
        Assert.assertEquals('N', tuple.L_RETURNFLAG);
        Assert.assertEquals('O', tuple.L_LINESTATUS);
        Assert.assertEquals(this.toDateInteger(1998, 4, 19), tuple.L_SHIPDATE);
        Assert.assertEquals(this.toDateInteger(1998, 6, 17), tuple.L_COMMITDATE);
        Assert.assertEquals(this.toDateInteger(1998, 4, 21), tuple.L_RECEIPTDATE);
        Assert.assertEquals("TAKE BACK RETURN         ", tuple.L_SHIPINSTRUCT);
        Assert.assertEquals("AIR       ", tuple.L_SHIPMODE);
        Assert.assertEquals("express instructions among the excuses nag", tuple.L_COMMENT);
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
