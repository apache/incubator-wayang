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
import org.apache.wayang.apps.tpch.data.PartSupplierTuple;
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
public class Query42 {

     /**
     * @param lineItemUrl URL to the lineitem CSV file
     * @param partSupplier URL to the nation CSV file
     * @param delta       the {@code [DELTA]} parameter
     * @return {@link WayangPlan} that implements the query
     */
    public static WayangPlan createPlan(
        String lineItemUrl,
        String partSupplierUrl,
        final int delta
    ) {
        // Read the tables from files.
        TextFileSource lineItemText = new TextFileSource(lineItemUrl, "UTF-8");
        TextFileSource partSupplierText = new TextFileSource(partSupplierUrl, "UTF-8");

        // Parse the rows.
        MapOperator<String, LineItemTuple> lineItemParser = new MapOperator<>(
            (line) -> new LineItemTuple.Parser().parse(line, '|'), String.class, LineItemTuple.class
        );
        lineItemText.connectTo(0, lineItemParser, 0);

        MapOperator<String, PartSupplierTuple> partSupplierParser = new MapOperator<>(
            (line) -> new PartSupplierTuple.Parser().parse(line, '|'), String.class, PartSupplierTuple.class
        );
        partSupplierText.connectTo(0, partSupplierParser, 0);

        // Execute co-lineitem Join
        JoinOperator<LineItemTuple, PartSupplierTuple, Long> join1 = new JoinOperator<>(
            (line) -> line.L_SUPPKEY,
            (partSupp) -> partSupp.PS_SUPPLKEY,
            LineItemTuple.class,
            PartSupplierTuple.class,
            Long.class
        );

        lineItemParser.connectTo(0, join1, 0);
        partSupplierParser.connectTo(0, join1, 1);

        LocalCallbackSink<Tuple2<LineItemTuple, PartSupplierTuple>> sink = LocalCallbackSink.createStdoutSink(ReflectionUtils.specify(Tuple2.class));
        join1.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }
}
