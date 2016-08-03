package org.qcri.rheem.tests;

import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.graphchi.plugin.GraphChiPlugin;
import org.qcri.rheem.java.plugin.JavaBasicPlugin;
import org.qcri.rheem.spark.plugin.SparkBasicPlugin;

/**
 * Integration tests for the integration of GraphChi with Rheem.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();
        RheemContext rc = new RheemContext().with(new JavaBasicPlugin()).with(new GraphChiPlugin());
        rc.execute(rheemPlan);
    }

    @Test
    public void testPageRankWithSpark() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();
        RheemContext rc = new RheemContext().with(new SparkBasicPlugin()).with(new GraphChiPlugin());
        rc.execute(rheemPlan);
    }

}
