package org.apache.incubator.wayang.tests;

import org.junit.Assert;
import org.junit.Test;
import org.apache.incubator.wayang.basic.data.Tuple2;
import org.apache.incubator.wayang.core.api.WayangContext;
import org.apache.incubator.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.incubator.wayang.giraph.Giraph;
import org.apache.incubator.wayang.graphchi.GraphChi;
import org.apache.incubator.wayang.java.Java;
import org.apache.incubator.wayang.spark.Spark;
import org.apache.incubator.wayang.tests.WayangPlans;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration tests for the integration of GraphChi with Wayang.
 */
public class GiraphIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.pageRankWithDictionaryCompression(pageRanks);
        WayangContext rc = new WayangContext().with(Java.basicPlugin()).with(Giraph.plugin());
        rc.execute(wayangPlan);

        pageRanks.stream().forEach(System.out::println);
        this.check(pageRanks);
    }


    @Test
    public void testPageRankWithoutGiraph() {
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
        solutions.forEach((k, v) -> Assert.assertTrue(String.format("Missing page rank for %s.", k), vertices.contains(k)));
        Assert.assertEquals(String.format("Illegal number of page ranks in %s.", pageRanks), solutions.size(), pageRanks.size());
    }

}
