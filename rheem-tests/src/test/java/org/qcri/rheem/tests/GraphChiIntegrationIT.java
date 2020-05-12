package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.graphchi.GraphChi;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Integration tests for the integration of GraphChi with Rheem.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlans.pageRankWithDictionaryCompression(pageRanks);
        RheemContext rc = new RheemContext().with(Java.basicPlugin()).with(GraphChi.plugin());
        rc.execute(rheemPlan);

        // Seems like a bug in GraphChi: One too many vertices.
        pageRanks = pageRanks.stream().filter(pr -> pr.field0 != null).collect(Collectors.toList());
        this.check(pageRanks);

    }

    @Test
    public void testPageRankWithSpark() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlans.pageRankWithDictionaryCompression(pageRanks);
        RheemContext rc = new RheemContext().with(Spark.basicPlugin())
                .with(Java.channelConversionPlugin())
                .with(GraphChi.plugin());
        rc.execute(rheemPlan);

        // Seems like a bug in GraphChi: One too many vertices.
        pageRanks = pageRanks.stream().filter(pr -> pr.field0 != null).collect(Collectors.toList());
        this.check(pageRanks);
    }

    @Test
    public void testPageRankWithoutGraphChi() {
        List<Tuple2<Character, Float>> pageRanks = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlans.pageRankWithDictionaryCompression(pageRanks);
        RheemContext rc = new RheemContext().with(Spark.basicPlugin())
                .with(Java.basicPlugin())
                .with(Java.graphPlugin());
        rc.execute(rheemPlan);

        this.check(pageRanks);
    }

    private void check(List<Tuple2<Character, Float>> pageRanks) {
        final Map<Character, Float> solutions = RheemPlans.pageRankWithDictionaryCompressionSolution();
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
