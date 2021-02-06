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

package org.apache.wayang.tests;

import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.graphchi.GraphChi;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration tests for the integration of GraphChi with Wayang.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext().with(Java.basicPlugin()).with(GraphChi.plugin());
        rc.execute(wayangPlan);

        // Seems like a bug in GraphChi: One too many vertices.
        pageRanks = pageRanks.stream().filter(pr -> pr.field0 != null).collect(Collectors.toList());
        this.check(pageRanks);

    }

    @Test
    public void testPageRankWithSpark() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext().with(Spark.basicPlugin())
                .with(Java.channelConversionPlugin())
                .with(GraphChi.plugin());
        rc.execute(wayangPlan);

        // Seems like a bug in GraphChi: One too many vertices.
        pageRanks = pageRanks.stream().filter(pr -> pr.field0 != null).collect(Collectors.toList());
        this.check(pageRanks);
    }

    @Test
    public void testPageRankWithoutGraphChi() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext().with(Spark.basicPlugin())
                .with(Java.basicPlugin())
                .with(Java.graphPlugin());
        rc.execute(wayangPlan);

        this.check(pageRanks);
    }

    private void check(List<Tuple2<Character, Float>> pageRanks) {
        final Map<Character, Float> solutions = WayangPlans.pageRankWithDictionaryCompressionSolution();
        Set<Character> vertices = pageRanks.stream().map(Tuple2::getField0).collect(Collectors.toSet());
        solutions.forEach((k, v) -> Assert.assertTrue(String.format("Missing page rank for %s (got: %s).", k, pageRanks), vertices.contains(k)));
        Assert.assertEquals(String.format("Illegal number of page ranks in %s.", pageRanks), solutions.size(), pageRanks.size());
        for (int i = 0; i < pageRanks.size(); i++) {
            char ci = pageRanks.get(i).field0;
            float fi = pageRanks.get(i).field1;
            float gi = solutions.get(ci);

            for (int j = i + 1; j < pageRanks.size(); j++) {
                char cj = pageRanks.get(j).field0;
                float fj = pageRanks.get(j).field1;
                float gj = solutions.get(cj);

                Assert.assertEquals(
                        (int) Math.signum(Float.compare(fi, fj)),
                        (int) Math.signum(Float.compare(gi, gj))
                );
            }
        }
    }
}
