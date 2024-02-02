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
import org.apache.wayang.apps.tpch.data.NationTuple;
import org.apache.wayang.apps.tpch.data.q10.QueryResultTuple;
import org.apache.wayang.apps.tpch.data.q10.GroupKey;
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
public class Query10 {

    /**
     * Creates TPC-H Query 10, which is as follows:
     * <pre>
     * SELECT
     *     c_custkey,
     *     c_name,
     *     sum(l_extendedprice * (1 - l_discount)) as revenue,
     *     c_acctbal,
     *     n_name,
     *     c_address,
     *     c_phone,
     *     c_comment
     * FROM
     *     customer,
     *     orders,
     *     lineitem,
     *     nation
     * WHERE
     *     c_custkey = o_custkey
     *     AND l_orderkey = o_orderkey
     *     AND o_orderdate >= date '1993-10-01'
     *     AND o_orderdate < date '1993-10-01' + interval '3' month
     *     AND l_returnflag = 'R'
     *     AND c_nationkey = n_nationkey
     * GROUP BY
     *     c_custkey,
     *     c_name,
     *     c_acctbal,
     *     c_phone,
     *     n_name,
     *     c_address,
     *     c_comment
     * ORDER BY
     *     revenue desc
     * LIMIT 20;
     * </pre>
     *
     * @param customer URL to the customer CSV file
     * @param order URL to the order CSV file
     * @param lineItemUrl URL to the lineitem CSV file
     * @param nationURL to the nation CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(
        String customerUrl,
        String orderUrl,
        String lineItemUrl,
        String nationUrl,
        final int delta
    ) {
        // Read the tables from files.
        TextFileSource customerText = new TextFileSource(customerUrl, "UTF-8");
        TextFileSource orderText = new TextFileSource(orderUrl, "UTF-8");
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");
        TextFileSource nationText = new TextFileSource(nationUrl, "UTF-8");

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

        MapOperator<String, NationTuple> nationParser = new MapOperator<>(
                (line) -> new NationTuple.Parser().parse(line, '|'), String.class, NationTuple.class
        );
        nationText.connectTo(0, nationParser, 0);

        // Execute customer-order Join
        JoinOperator<CustomerTuple, OrderTuple, Integer> coJoin = new JoinOperator<CustomerTuple, OrderTuple, Integer>(
            (customer) -> customer.L_CUSTKEY,
            (order) -> order.L_CUSTKEY,
            CustomerTuple.class,
            OrderTuple.class,
            Integer.class
        );

        customerParser.connectTo(0, coJoin, 0);
        orderParser.connectTo(0, coJoin, 1);

        // Execute co-lineitem Join
        JoinOperator<LineItemTuple, Tuple2<CustomerTuple, OrderTuple>, Integer> coLJoin = new JoinOperator<>(
            (line) -> (int) line.L_ORDERKEY,
            (tuple) -> tuple.field1.L_ORDERKEY,
            LineItemTuple.class,
            ReflectionUtils.specify(Tuple2.class),
            Integer.class
        );

        lineItemParser.connectTo(0, coLJoin, 0);
        coJoin.connectTo(0, coLJoin, 1);

        // Filter.
        final int minDate = LineItemTuple.Parser.parseDate("1993-10-01") - delta;
        final int maxDate = LineItemTuple.Parser.parseDate("1994-01-01") - delta;
        FilterOperator<Tuple2<LineItemTuple, Tuple2<CustomerTuple, OrderTuple>>> filter = new FilterOperator<>(
                (tuple) -> tuple.field1.field1.L_ORDERDATE >= minDate && tuple.field1.field1.L_ORDERDATE < maxDate && tuple.field0.L_RETURNFLAG == 'R',
                ReflectionUtils.specify(Tuple2.class)
        );

        coLJoin.connectTo(0, filter, 0);

        JoinOperator<Tuple2<LineItemTuple, Tuple2<CustomerTuple, OrderTuple>>, NationTuple, Integer> nationJoin = new JoinOperator<>(
            (tuple) -> tuple.field1.field0.L_NATIONKEY,
            (nation) -> nation.N_NATIONKEY,
            ReflectionUtils.specify(Tuple2.class),
            NationTuple.class,
            Integer.class
        );

        filter.connectTo(0, nationJoin, 0);
        nationParser.connectTo(0, nationJoin, 1);

        MapOperator<Tuple2<Tuple2<LineItemTuple, Tuple2<CustomerTuple, OrderTuple>>, NationTuple>, QueryResultTuple> resultProjection = new MapOperator<>(
                ((tuple) -> {
                    CustomerTuple customer = tuple.field0.field1.field0;
                    LineItemTuple line = tuple.field0.field0;
                    NationTuple nation = tuple.field1;

                    return new QueryResultTuple(
                        customer.L_CUSTKEY,
                        customer.L_NAME,
                        line.L_EXTENDEDPRICE * (1 - line.L_DISCOUNT),
                        customer.L_ACCTBAL,
                        nation.N_NAME,
                        customer.L_ADDRESS,
                        customer.L_PHONE,
                        customer.L_COMMENT
                    );
                }),
                ReflectionUtils.specify(Tuple2.class),
                QueryResultTuple.class
        );
        nationJoin.connectTo(0, resultProjection, 0);

        ReduceByOperator<QueryResultTuple, GroupKey> aggregation = new ReduceByOperator<>(
                ((returnTuple) -> {
                    return new GroupKey(
                        returnTuple.C_CUSTKEY,
                        returnTuple.C_NAME,
                        returnTuple.C_ACCTBAL,
                        returnTuple.C_PHONE,
                        returnTuple.N_NAME,
                        returnTuple.C_ADDRESS,
                        returnTuple.C_COMMENT
                    );
                }),
                ((t1, t2) -> {
                    t1.REVENUE = t2.REVENUE;
                    return t1;
                }),
                GroupKey.class,
                QueryResultTuple.class
        );

        // Print the results.
        resultProjection.connectTo(0, aggregation, 0);

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
