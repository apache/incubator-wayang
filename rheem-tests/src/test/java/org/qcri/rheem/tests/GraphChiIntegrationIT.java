package org.qcri.rheem.tests;

import org.junit.Test;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.profiling.FullInstrumentationStrategy;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * Integration tests for the integration of GraphChi with Rheem.
 */
public class GraphChiIntegrationIT {

    @Test
    public void testPageRankWithJava() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();

        RheemContext rc = new RheemContext();
        rc.register(JavaPlatform.getInstance());
        rc.register(GraphChiPlatform.getInstance());
        rc.getConfiguration().getInstrumentationStrategyProvider().set(new FullInstrumentationStrategy());

        rc.execute(rheemPlan);

    }

    @Test
    public void testPageRankWithSpark() {
        RheemPlan rheemPlan = RheemPlans.createCrossCommunityPageRank();

        RheemContext rc = new RheemContext();
        rc.register(SparkPlatform.getInstance());
        rc.register(GraphChiPlatform.getInstance());

        rc.execute(rheemPlan);

    }

}
