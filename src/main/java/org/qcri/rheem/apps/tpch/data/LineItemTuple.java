package org.qcri.rheem.apps.tpch.data;

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

        public LineItemTuple parse(String line) {
            LineItemTuple tuple = new LineItemTuple();

            int startPos = 0;
            int endPos = line.indexOf(';', startPos);
            tuple.L_ORDERKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_PARTKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_SUPPKEY = Long.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_LINENUMBER = Integer.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_QUANTITY = Double.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_EXTENDEDPRICE = Double.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_DISCOUNT = Double.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_TAX = Double.valueOf(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_RETURNFLAG = line.charAt(startPos + 1);

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_LINESTATUS = line.charAt(startPos + 1);

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_SHIPDATE = parseDate(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_COMMITDATE = parseDate(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = line.indexOf(';', startPos);
            tuple.L_RECEIPTDATE = parseDate(line.substring(startPos + 1, endPos - 1));

            startPos = endPos + 1;
            endPos = startPos - 1;
            do {
                endPos++;
                endPos = line.indexOf(';', endPos);
            } while (line.charAt(endPos - 1) != '"' || line.charAt(endPos + 1) != '"');
            tuple.L_SHIPINSTRUCT = line.substring(startPos + 1, endPos - 1);

            startPos = endPos + 1;
            endPos = startPos - 1;
            do {
                endPos++;
                endPos = line.indexOf(';', endPos);
            } while (line.charAt(endPos - 1) != '"' || line.charAt(endPos + 1) != '"');
            tuple.L_SHIPMODE = line.substring(startPos + 1, endPos - 1);

            startPos = endPos + 1;
            endPos = startPos - 1;
            do {
                endPos++;
                endPos = line.indexOf(';', endPos);
            } while (endPos >= 0 && (line.charAt(endPos - 1) != '"' || (endPos < line.length() - 1 && line.charAt(endPos + 1) != '"')));
            assert endPos < 0 : String.format("Parsing error: unexpected ';' at %d. Input: %s", endPos, line);
            endPos = line.length();
            tuple.L_COMMENT = line.substring(startPos + 1, endPos - 1);

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
