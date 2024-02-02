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
import org.apache.wayang.apps.tpch.data.SupplierTuple;
import org.apache.wayang.apps.tpch.data.NationTuple;
import org.apache.wayang.apps.tpch.data.RegionTuple;
import org.apache.wayang.apps.tpch.data.q5.LineItemReturnTuple;
import org.apache.wayang.apps.tpch.data.q5.QueryResultTuple;
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
/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Query5 {

    /**
     * Creates TPC-H Query 5, which is as follows:
     * <pre>
     * SELECT
     *     n_name,
     *     sum(l_extendedprice * (1 - l_discount)) as revenue
     * FROM
     *     customer,
     *     orders,
     *     lineitem,
     *     supplier,
     *     nation,
     *     region
     * WHERE
     *     c_custkey = o_custkey
     *     AND l_orderkey = o_orderkey
     *     AND l_suppkey = s_suppkey
     *     AND c_nationkey = s_nationkey
     *     AND s_nationkey = n_nationkey
     *     AND n_regionkey = r_regionkey
     *     AND r_name = 'ASIA'
     *     AND o_orderdate >= date '1994-01-01'
     *     AND o_orderdate < date '1994-01-01' + interval '1' year
     * GROUP BY
     *     n_name
     * ORDER BY
     *     revenue desc;
     * </pre>
     *
     * @param customer URL to the customer CSV file
     * @param order URL to the order CSV file
     * @param lineItemUrl URL to the lineitem CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(
            String customerUrl,
            String orderUrl,
            String lineItemUrl,
            String supplierUrl,
            String nationUrl,
            String regionUrl,
            final int delta
    ) {
        // Read the tables from files.
        TextFileSource customerText = new TextFileSource(customerUrl, "UTF-8");
        TextFileSource orderText = new TextFileSource(orderUrl, "UTF-8");
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");
        TextFileSource supplierText = new TextFileSource(supplierUrl, "UTF-8");
        TextFileSource nationText = new TextFileSource(nationUrl, "UTF-8");
        TextFileSource regionText = new TextFileSource(regionUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> lineItemParser = new MapOperator<>(
                (line) -> new LineItemTuple.Parser().parse(line, '|'), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, lineItemParser, 0);

        MapOperator<String, OrderTuple> orderParser = new MapOperator<>(
                (line) -> new OrderTuple.Parser().parse(line, '|'), String.class, OrderTuple.class
        );
        orderText.connectTo(0, orderParser, 0);

        MapOperator<String, CustomerTuple> customerParser = new MapOperator<>(
                (line) -> new CustomerTuple.Parser().parse(line, '|'), String.class, CustomerTuple.class
        );
        customerText.connectTo(0, customerParser, 0);

        MapOperator<String, SupplierTuple> supplierParser = new MapOperator<>(
                (line) -> new SupplierTuple.Parser().parse(line, '|'), String.class, SupplierTuple.class
        );
        supplierText.connectTo(0, supplierParser, 0);

        MapOperator<String, NationTuple> nationParser = new MapOperator<>(
                (line) -> new NationTuple.Parser().parse(line, '|'), String.class, NationTuple.class
        );
        nationText.connectTo(0, nationParser, 0);

        MapOperator<String, RegionTuple> regionParser = new MapOperator<>(
                (line) -> new RegionTuple.Parser().parse(line, '|'), String.class, RegionTuple.class
        );
        regionText.connectTo(0, regionParser, 0);

        // Filter regions.
        FilterOperator<RegionTuple> regionFilter = new FilterOperator<>(
                (tuple) -> tuple.R_NAME.contains("ASIA"), RegionTuple.class
        );
        regionParser.connectTo(0, regionFilter, 0);

        // Filter orders.
        final int maxOrderdate = OrderTuple.Parser.parseDate("1995-01-01") - delta;
        final int minOrderdate = OrderTuple.Parser.parseDate("1994-01-01") - delta;
        FilterOperator<OrderTuple> orderFilter = new FilterOperator<>(
                (tuple) -> tuple.L_ORDERDATE < maxOrderdate && tuple.L_ORDERDATE >= minOrderdate,
                OrderTuple.class
        );
        orderParser.connectTo(0, orderFilter, 0);

        // Project the queried attributes.
        MapOperator<LineItemTuple, LineItemReturnTuple> lineProjection = new MapOperator<>(
                (lineItemTuple) -> new LineItemReturnTuple(
                        lineItemTuple.L_ORDERKEY,
                        lineItemTuple.L_SUPPKEY,
                        lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT)
                ),
                LineItemTuple.class,
                LineItemReturnTuple.class
        );
        lineItemParser.connectTo(0, lineProjection, 0);

        // Project the queried attributes.
        MapOperator<NationTuple, String> nationProjection = new MapOperator<NationTuple, String>(
                (tuple) -> tuple.N_NAME,
                NationTuple.class,
                String.class
        );
        nationParser.connectTo(0, nationProjection, 0);

        // Execute customer-order Join
        JoinOperator<CustomerTuple, OrderTuple, Integer> coJoin = new JoinOperator<CustomerTuple, OrderTuple, Integer>(
            (customer) -> customer.L_CUSTKEY,
            (order) -> order.L_CUSTKEY,
            CustomerTuple.class,
            OrderTuple.class,
            Integer.class
        );

        customerParser.connectTo(0, coJoin, 0);
        orderFilter.connectTo(0, coJoin, 1);

        // Execute customer-order with line-items Join
        JoinOperator<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>, Integer> coLJoin = new JoinOperator<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>, Integer>(
            (lineItemReturnTuple) -> (int) lineItemReturnTuple.L_ORDERKEY,
            (tuple) -> tuple.field1.L_ORDERKEY,
            LineItemReturnTuple.class,
            ReflectionUtils.specify(Tuple2.class),
            Integer.class
        );

        lineProjection.connectTo(0, coLJoin, 0);
        coJoin.connectTo(0, coLJoin, 1);

        // Execute C-O-L with supplier Join
        JoinOperator<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple, Long> colsJoin =
            new JoinOperator<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple, Long>(
            (tuple) -> tuple.field0.L_SUPPKEY,
            (supplier) -> supplier.S_SUPPKEY,
            ReflectionUtils.specify(Tuple2.class),
            SupplierTuple.class,
            Long.class
        );

        coLJoin.connectTo(0, colsJoin, 0);
        supplierParser.connectTo(0, colsJoin, 1);

        // Execute second C-O-L-S with supplier Join
        JoinOperator<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple, Integer> colsJoinTwo =
            new JoinOperator<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple, Integer>(
                (tuple) -> tuple.field0.field1.field0.L_NATIONKEY,
                (supplier) -> supplier.S_NATIONKEY,
                ReflectionUtils.specify(Tuple2.class),
                SupplierTuple.class,
                Integer.class
            );

        colsJoin.connectTo(0, colsJoinTwo, 0);
        supplierParser.connectTo(0, colsJoinTwo, 1);

        // Execute C-O-L-S with nation Join
        JoinOperator<Tuple2<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple>, NationTuple, Integer> colsNJoin =
            new JoinOperator<Tuple2<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple>, NationTuple, Integer>(
                (tuple) -> tuple.field1.S_NATIONKEY,
                (nation) -> nation.N_NATIONKEY,
                ReflectionUtils.specify(Tuple2.class),
                NationTuple.class,
                Integer.class
            );

        colsJoinTwo.connectTo(0, colsNJoin, 0);
        nationParser.connectTo(0, colsNJoin, 1);

        // Execute C-O-L-S-N with region Join
        JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple>, NationTuple>, RegionTuple, Integer> colsnRJoin =
            new JoinOperator<Tuple2<Tuple2<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple>, NationTuple>, RegionTuple, Integer>(
                (tuple) -> tuple.field1.N_REGIONKEY,
                (region) -> region.R_REGIONKEY,
                ReflectionUtils.specify(Tuple2.class),
                RegionTuple.class,
                Integer.class
            );

        colsNJoin.connectTo(0, colsnRJoin, 0);
        regionFilter.connectTo(0, colsnRJoin, 1);

        MapOperator<Tuple2<Tuple2<Tuple2<Tuple2<Tuple2<LineItemReturnTuple, Tuple2<CustomerTuple, OrderTuple>>, SupplierTuple>, SupplierTuple>, NationTuple>, RegionTuple>, QueryResultTuple> joinProjection = new MapOperator<>(
                (join) -> new QueryResultTuple(
                    join.field0.field1.N_NAME,
                    join.field0.field0.field0.field0.field0.L_EXTENDEDPRICE
                ),
                ReflectionUtils.specify(Tuple2.class),
                QueryResultTuple.class
        );
        colsnRJoin.connectTo(0, joinProjection, 0);

        ReduceByOperator<QueryResultTuple, String> aggregation = new ReduceByOperator<>(
                (returnTuple) -> returnTuple.N_NAME,
                ((t1, t2) -> {
                    t1.REVENUE += t2.REVENUE;
                    return t1;
                }),
                String.class,
                QueryResultTuple.class
        );

        // Print the results.
        joinProjection.connectTo(0, aggregation, 0);

        SortOperator<QueryResultTuple, Double> sort = new SortOperator<>(
                tuple -> tuple.REVENUE,
                QueryResultTuple.class,
                Double.class
        );
        aggregation.connectTo(0, sort, 0);

        LocalCallbackSink<QueryResultTuple> sink = LocalCallbackSink.createStdoutSink(QueryResultTuple.class);
        sort.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
