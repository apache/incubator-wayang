package org.qcri.rheem.tests;

import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.graphchi.plugin.GraphChiPlugin;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * Integration tests for the integration of GraphChi with Rheem.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();
        RheemContext rc = new RheemContext().with(JavaPlatform.getInstance()).with(new GraphChiPlugin());
        rc.execute(rheemPlan);
    }

    @Test
    public void testPageRankWithSpark() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();
        RheemContext rc = new RheemContext().with(SparkPlatform.getInstance()).with(new GraphChiPlugin());
        rc.execute(rheemPlan);
    }

}
