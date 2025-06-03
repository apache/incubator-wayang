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

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.giraph.Giraph;
import org.apache.wayang.java.Java;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the integration of Giraph with Wayang.
 */
class GiraphIntegrationIT {

    //TODO validate if this test is helpfull
    @Disabled
    @Test
    void testPageRankWithJava() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext().with(Java.basicPlugin()).with(Giraph.plugin());
        rc.execute(wayangPlan);

        pageRanks.forEach(System.out::println);
        this.check(pageRanks);
    }


    @Test
    void testPageRankWithoutGiraph() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext()
                .with(Java.basicPlugin())
                .with(Java.graphPlugin());
        rc.execute(wayangPlan);

        this.check(pageRanks);
    }

    private void check(List<Tuple2<Character, Float>> pageRanks) {
        final Map<Character, Float> solutions = WayangPlans.pageRankWithDictionaryCompressionSolution();
        Set<Character> vertices = pageRanks.stream().map(Tuple2::getField0).collect(Collectors.toSet());
        solutions.forEach((k, v) -> assertTrue(vertices.contains(k), String.format("Missing page rank for %s.", k)));
        assertEquals(solutions.size(), pageRanks.size(), String.format("Illegal number of page ranks in %s.", pageRanks));
    }

}
