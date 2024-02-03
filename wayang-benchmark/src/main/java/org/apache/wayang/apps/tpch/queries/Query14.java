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

package org.apache.wayang.apps.tpch.queries;

import org.apache.wayang.apps.tpch.data.LineItemTuple;
import org.apache.wayang.apps.tpch.data.OrderTuple;
import org.apache.wayang.apps.tpch.data.CustomerTuple;
import org.apache.wayang.apps.tpch.data.q14.LineItemReturnTuple;
import org.apache.wayang.apps.tpch.data.q14.PromoSumTuple;
import org.apache.wayang.apps.tpch.data.PartTuple;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.basic.operators.GlobalReduceOperator;

/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Query14 {

    /**
     * Creates TPC-H Query 14, which is as follows:
     * <pre>
     * SELECT
     *     100.00 * sum(case
     *         when p_type like 'PROMO%'
     *             then l_extendedprice * (1 - l_discount)
     *         else 0
     *     end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
     * FROM
     *     lineitem,
     *     part
     * WHERE
     *     l_partkey = p_partkey
     *     AND l_shipdate >= date '1995-09-01'
     *     AND l_shipdate < date '1995-09-01' + interval '1' month;
     * </pre>
     *
     * @param lineItemUrl URL to the lineitem CSV file
     * @param partUrl URL to the part CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(String lineItemUrl, String partUrl, final int delta) {
        // Read the tables from files.
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");
        TextFileSource partText = new TextFileSource(partUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> lineItemParser = new MapOperator<>(
                (line) -> new LineItemTuple.Parser().parse(line, '|'), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, lineItemParser, 0);

        MapOperator<String, PartTuple> partParser = new MapOperator<>(
                (line) -> new PartTuple.Parser().parse(line, '|'), String.class, PartTuple.class
        );
        partText.connectTo(0, partParser, 0);

        // Filter by shipdate.
        final int minShipdate = LineItemTuple.Parser.parseDate("1995-09-01") - delta;
        final int maxShipdate = LineItemTuple.Parser.parseDate("1995-10-01") - delta;
        FilterOperator<LineItemTuple> lineFilter = new FilterOperator<>(
                (tuple) -> tuple.L_SHIPDATE >= minShipdate && tuple.L_SHIPDATE < maxShipdate, LineItemTuple.class
        );
        lineItemParser.connectTo(0, lineFilter, 0);

        // Project the queried attributes.
        MapOperator<LineItemTuple, LineItemReturnTuple> lineProjection = new MapOperator<>(
                (lineItemTuple) -> new LineItemReturnTuple(
                        lineItemTuple.L_PARTKEY,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT)
                ),
                LineItemTuple.class,
                LineItemReturnTuple.class
        );
        lineFilter.connectTo(0, lineProjection, 0);

        JoinOperator<LineItemReturnTuple, PartTuple, Long> lpJoin = new JoinOperator<LineItemReturnTuple, PartTuple, Long>(
            (line) -> line.L_PARTKEY,
            (part) -> part.P_PARTKEY,
            LineItemReturnTuple.class,
            PartTuple.class,
            Long.class
        );
        lineProjection.connectTo(0, lpJoin, 0);
        partParser.connectTo(0, lpJoin, 1);

        // Project the queried attributes.
        MapOperator<Tuple2<LineItemReturnTuple, PartTuple>, PromoSumTuple> sumProjection = new MapOperator<>(
                ((tuple) -> {
                    double promoPrice = 0;
                    if (tuple.field1.P_TYPE.contains("PROMO"))  {
                        promoPrice = tuple.field0.L_EXTENDEDPRICE;
                    }

                    PromoSumTuple result = new PromoSumTuple();
                    result.L_EXTENDEDPRICE = tuple.field0.L_EXTENDEDPRICE;
                    result.L_PROMO_EXTENDEDPRICE = promoPrice;

                    return result;
                }),
                ReflectionUtils.specify(Tuple2.class),
                PromoSumTuple.class
        );
        lpJoin.connectTo(0, sumProjection, 0);

        GlobalReduceOperator<PromoSumTuple> sumReduction =
            new GlobalReduceOperator<PromoSumTuple>(
                ((t1, t2) -> {
                   t1.L_PROMO_EXTENDEDPRICE += t2.L_PROMO_EXTENDEDPRICE;
                   t1.L_EXTENDEDPRICE += t2.L_EXTENDEDPRICE;

                   return t1;
                }),
                PromoSumTuple.class
            );

        sumProjection.connectTo(0, sumReduction, 0);

        MapOperator<PromoSumTuple, Double> resultSum = new MapOperator<>(
            (tuple) -> 100 * (tuple.L_PROMO_EXTENDEDPRICE / tuple.L_EXTENDEDPRICE),
            PromoSumTuple.class,
            Double.class
        );

        sumReduction.connectTo(0, resultSum, 0);

        LocalCallbackSink<Double> sink = LocalCallbackSink.createStdoutSink(Double.class);
        resultSum.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
