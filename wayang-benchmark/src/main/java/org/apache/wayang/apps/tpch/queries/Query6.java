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
public class Query6 {

    /**
     * Creates TPC-H Query 6, which is as follows:
     * <pre>
     * SELECT
     *     sum(l_extendedprice * l_discount) as revenue
     * FROM
     *     lineitem
     * WHERE
     *     l_shipdate >= date '1994-01-01'
     *     AND l_shipdate < date '1994-01-01' + interval '1' year
     *     AND l_discount between 0.06 - 0.01 AND 0.06 + 0.01
     *     AND l_quantity < 24;
     * </pre>
     *
     * @param lineItemUrl URL to the lineitem CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(String lineItemUrl, final int delta) {
        // Read the tables from files.
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> lineItemParser = new MapOperator<>(
                (line) -> new LineItemTuple.Parser().parse(line, '|'), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, lineItemParser, 0);

        // Filter by shipdate.
        final int minShipdate = LineItemTuple.Parser.parseDate("1994-01-01") - delta;
        final int maxShipdate = OrderTuple.Parser.parseDate("1995-01-01") - delta;
        FilterOperator<LineItemTuple> lineFilter = new FilterOperator<>(
                (tuple) -> tuple.L_SHIPDATE >= minShipdate &&
                    tuple.L_SHIPDATE < maxShipdate &&
                    tuple.L_DISCOUNT > 0.05 &&
                    tuple.L_DISCOUNT < 0.07 &&
                    tuple.L_QUANTITY < 24,
                LineItemTuple.class
        );
        lineItemParser.connectTo(0, lineFilter, 0);

        // Project the queried attributes.
        MapOperator<LineItemTuple, Double> lineProjection = new MapOperator<>(
                (lineItemTuple) -> lineItemTuple.L_EXTENDEDPRICE * (1 - lineItemTuple.L_DISCOUNT),
                LineItemTuple.class,
                Double.class
        );
        lineFilter.connectTo(0, lineProjection, 0);

       // Print the results.
        LocalCallbackSink<Double> sink = LocalCallbackSink.createStdoutSink(Double.class);
        lineProjection.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
