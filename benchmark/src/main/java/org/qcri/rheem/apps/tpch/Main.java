package org.qcri.rheem.apps.tpch;

import org.qcri.rheem.apps.tpch.data.LineItemTuple;
import org.qcri.rheem.apps.tpch.data.q1.GroupKey;
import org.qcri.rheem.apps.tpch.data.q1.ReturnTuple;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * Main class for the TPC-H app based on Rheem.
 */
public class Main {

    /**
     * Creates TPC-H Query 1, which is as follows:
     * <pre>
     * select
     *  l_returnflag,
     *  l_linestatus,
     *  sum(l_quantity) as sum_qty,
     *  sum(l_extendedprice) as sum_base_price,
     *  sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
     *  sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
     *  avg(l_quantity) as avg_qty,
     *  avg(l_extendedprice) as avg_price,
     *  avg(l_discount) as avg_disc,
     *  count(*) as count_order
     * from
     *  lineitem
     * where
     *  l_shipdate <= date '1998-12-01' - interval '[DELTA]' day (3)
     * group by
     *  l_returnflag,
     *  l_linestatus
     * order by
     *  l_returnflag,
     *  l_linestatus;
     * </pre>
     *
     * @param lineItemUrl URL to the lineitem CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link RheemPlan} that implements the query
     */
    private static RheemPlan createQ1(String lineItemUrl, final int delta) {
        // Read the lineitem table.
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> parser = new MapOperator<>(
                (line) -> new LineItemTuple.Parser().parse(line), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, parser, 0);

        // Filter by shipdate.
        final int maxShipdate = LineItemTuple.Parser.parseDate("1998-12-01") - delta;
        FilterOperator<LineItemTuple> filter = new FilterOperator<>(
                (tuple) -> tuple.L_SHIPDATE <= maxShipdate, LineItemTuple.class
        );
        parser.connectTo(0, filter, 0);

        // Project the queried attributes.
        MapOperator<LineItemTuple, ReturnTuple> projection = new MapOperator<>(
                (lineItemTuple) -> new ReturnTuple(
                        lineItemTuple.L_RETURNFLAG,
                        lineItemTuple.L_LINESTATUS,
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT) * (1 + lineItemTuple.L_TAX),
                        lineItemTuple.L_QUANTITY,
                        lineItemTuple.L_EXTENDEDPRICE,
                        lineItemTuple.L_DISCOUNT,
                        1),
                LineItemTuple.class,
                ReturnTuple.class
        );
        filter.connectTo(0, projection, 0);

        // Aggregation phase 1.
        ReduceByOperator<ReturnTuple, GroupKey> aggregation = new ReduceByOperator<>(
                (returnTuple) -> new GroupKey(returnTuple.L_RETURNFLAG, returnTuple.L_LINESTATUS),
                ((t1, t2) -> {
                    t1.SUM_QTY += t2.SUM_QTY;
                    t1.SUM_BASE_PRICE += t2.SUM_BASE_PRICE;
                    t1.SUM_DISC_PRICE += t2.SUM_DISC_PRICE;
                    t1.SUM_CHARGE += t2.SUM_CHARGE;
                    t1.AVG_QTY += t2.AVG_QTY;
                    t1.AVG_PRICE += t2.AVG_PRICE;
                    t1.AVG_DISC += t2.AVG_DISC;
                    t1.COUNT_ORDER += t2.COUNT_ORDER;
                    return t1;
                }),
                GroupKey.class,
                ReturnTuple.class
        );
        projection.connectTo(0, aggregation, 0);

        // Aggregation phase 2: complete AVG operations.
        MapOperator<ReturnTuple, ReturnTuple> aggregationFinalization = new MapOperator<>(
                (t -> {
                    t.AVG_QTY /= t.COUNT_ORDER;
                    t.AVG_PRICE /= t.COUNT_ORDER;
                    t.AVG_DISC /= t.COUNT_ORDER;
                    return t;
                }),
                ReturnTuple.class,
                ReturnTuple.class
        );
        aggregation.connectTo(0, aggregationFinalization, 0);

        // TODO: Implement sorting (as of now not possible with Rheem's basic operators).

        // Print the results.
        LocalCallbackSink<ReturnTuple> sink = LocalCallbackSink.createStdoutSink(ReturnTuple.class);
        aggregationFinalization.connectTo(0, sink, 0);

        return new RheemPlan(sink);
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.print("Usage: <platform1>[,<platform2>]* <query number> <query parameters>*");
            System.exit(1);
        }

        RheemPlan rheemPlan;
        switch (Integer.parseInt(args[1])) {
            case 1:
                rheemPlan = createQ1(args[2], Integer.parseInt(args[3]));
                break;
            default:
                System.err.println("Unsupported query number.");
                System.exit(2);
                return;
        }

        RheemContext rheemContext = new RheemContext();
        for (String platform : args[0].split(",")) {
            switch (platform) {
                case "java":
                    rheemContext.register(Java.basicPlugin());
                    break;
                case "spark":
                    rheemContext.register(Spark.basicPlugin());
                    break;
                default:
                    System.err.format("Unknown platform: \"%s\"\n", platform);
                    System.exit(3);
                    return;
            }
        }

        rheemContext.execute(rheemPlan, ReflectionUtils.getDeclaringJar(Main.class), ReflectionUtils.getDeclaringJar(JavaPlatform.class));
    }
}
