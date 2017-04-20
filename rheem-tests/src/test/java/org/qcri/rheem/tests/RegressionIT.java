package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.LoadCollectionDataQuantaBuilder;
import org.qcri.rheem.api.MapDataQuantaBuilder;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class hosts and documents some tests for bugs that we encountered. Ultimately, we want to avoid re-introducing
 * already encountered and fixed bugs.
 */
public class RegressionIT {

    /**
     * This plan revealed an issue with the {@link org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph.ShortestTreeSearcher}.
     */
    @Test
    public void testCollectionToRddAndBroadcast() {
        RheemContext rheemContext = new RheemContext().with(Spark.basicPlugin()).with(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(rheemContext, "testCollectionToRddAndBroadcast");

        LoadCollectionDataQuantaBuilder<String> collection = planBuilder
                .loadCollection(Arrays.asList("a", "bc", "def"))
                .withTargetPlatform(Java.platform())
                .withName("collection");

        MapDataQuantaBuilder<String, Integer> map1 = collection
                .map(String::length)
                .withTargetPlatform(Spark.platform());

        MapDataQuantaBuilder<Integer, Integer> map2 = planBuilder
                .loadCollection(RheemArrays.asList(-1))

                .map(i -> i)
                .withBroadcast(collection, "broadcast")
                .withTargetPlatform(Spark.platform());

        ArrayList<Integer> result = new ArrayList<>(map1.union(map2).collect());

        result.sort(Integer::compareTo);
        Assert.assertEquals(
                RheemArrays.asList(-1, 1, 2, 3),
                result
        );
    }


}
