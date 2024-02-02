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

import java.util.Arrays;

/**
 * Main class for the TPC-H app based on Apache Wayang (incubating).
 */
public class Query19 {

    /**
     * Creates TPC-H Query 19, which is as follows:
     * <pre>
     * SELECT
     *     sum(l_extendedprice* (1 - l_discount)) as revenue
     * FROM
     *     lineitem,
     *     part
     * WHERE
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#12'
     *         AND p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
     *         AND l_quantity >= 1 AND l_quantity <= 1 + 10
     *         AND p_size between 1 AND 5
     *         AND l_shipmode in ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     )
     *     OR
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#23'
     *         AND p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
     *         AND l_quantity >= 10 AND l_quantity <= 10 + 10
     *         AND p_size between 1 AND 10
     *         AND l_shipmode in ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     )
     *     OR
     *     (
     *         p_partkey = l_partkey
     *         AND p_brand = 'Brand#34'
     *         AND p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
     *         AND l_quantity >= 20 AND l_quantity <= 20 + 10
     *         AND p_size between 1 AND 15
     *         AND l_shipmode in ('AIR', 'AIR REG')
     *         AND l_shipinstruct = 'DELIVER IN PERSON'
     *     );
     * </pre>
     *
     * @param lineItemUrl URL to the lineitem CSV file
     * @param  partUrl to the part CSV file
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

        // join part and lineitem
        JoinOperator<PartTuple, LineItemTuple, Long> jpJoin =
            new JoinOperator<PartTuple, LineItemTuple, Long>(
                (part) -> part.P_PARTKEY,
                (line) -> line.L_PARTKEY,
                PartTuple.class,
                LineItemTuple.class,
                Long.class
            );

        partParser.connectTo(0, jpJoin, 0);
        lineItemParser.connectTo(0, jpJoin, 1);

        //Filter the join product
        FilterOperator<Tuple2<PartTuple, LineItemTuple>> filter = new FilterOperator<>(
                ((tuple) -> {
                    String[] firstContainers = {"SM CASE", "SM BOX", "SM PACK", "SM PKG"};
                    String[] firstShipmodes = {"AIR", "AIR REG"};
                    boolean first = (
                        tuple.field0.P_BRAND.contains("Brand#12") &&
                        Arrays.asList(firstContainers).contains(tuple.field0.P_CONTAINER) &&
                        tuple.field1.L_QUANTITY >= 1 &&
                        tuple.field1.L_QUANTITY <= 11 &&
                        tuple.field0.P_SIZE >= 1 &&
                        tuple.field0.P_SIZE <= 5 &&
                        Arrays.asList(firstShipmodes).contains(tuple.field1.L_SHIPMODE) &&
                        tuple.field1.L_SHIPINSTRUCT.contains("DELIVER IN PERSON")
                    );
                    String[] secondContainers = {"MED BAG", "MED BOX", "MED PACK", "MED PKG"};
                    String[] secondShipModes = {"AIR", "AIR REG"};
                    boolean second = (
                        tuple.field0.P_BRAND.contains("Brand#23") &&
                        Arrays.asList(firstContainers).contains(tuple.field0.P_CONTAINER) &&
                        tuple.field1.L_QUANTITY >= 10 &&
                        tuple.field1.L_QUANTITY <= 20 &&
                        tuple.field0.P_SIZE >= 1 &&
                        tuple.field0.P_SIZE <= 10 &&
                        Arrays.asList(firstShipmodes).contains(tuple.field1.L_SHIPMODE) &&
                        tuple.field1.L_SHIPINSTRUCT.contains("DELIVER IN PERSON")
                    );
                    String[] thirdContainers = {"LG CASE", "LG BOX", "LG PACK", "LG PKG"};
                    String[] thirdShipmodes = {"AIR", "AIR REG"};
                    boolean third = (
                        tuple.field0.P_BRAND.contains("Brand#34") &&
                        Arrays.asList(firstContainers).contains(tuple.field0.P_CONTAINER) &&
                        tuple.field1.L_QUANTITY >= 20 &&
                        tuple.field1.L_QUANTITY <= 30 &&
                        tuple.field0.P_SIZE >= 1 &&
                        tuple.field0.P_SIZE <= 15 &&
                        Arrays.asList(firstShipmodes).contains(tuple.field1.L_SHIPMODE) &&
                        tuple.field1.L_SHIPINSTRUCT.contains("DELIVER IN PERSON")
                    );
                    return first || second || third;
                }),
                ReflectionUtils.specify(Tuple2.class)
        );
        jpJoin.connectTo(0, filter, 0);

        MapOperator<Tuple2<PartTuple, LineItemTuple>, Double> filterProjection = new MapOperator<>(
            (tuple) -> tuple.field1.L_EXTENDEDPRICE * (1 - tuple.field1.L_DISCOUNT),
            ReflectionUtils.specify(Tuple2.class),
            Double.class
        );

        filter.connectTo(0, filterProjection, 0);

        //Reduce to one sum
        GlobalReduceOperator<Double> sumReduction =
            new GlobalReduceOperator<Double>(
                (t1, t2) -> t1 + t2,
                Double.class
            );

        filterProjection.connectTo(0, sumReduction, 0);
        LocalCallbackSink<Double> sink = LocalCallbackSink.createStdoutSink(Double.class);
        sumReduction.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
