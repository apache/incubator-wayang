package io.rheem.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import io.rheem.rheem.api.JavaPlanBuilder;
import io.rheem.rheem.api.LoadCollectionDataQuantaBuilder;
import io.rheem.rheem.api.MapDataQuantaBuilder;
import io.rheem.rheem.core.api.RheemContext;
import io.rheem.rheem.core.util.RheemArrays;
import io.rheem.rheem.java.Java;
import io.rheem.rheem.spark.Spark;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * This class hosts and documents some tests for bugs that we encountered. Ultimately, we want to avoid re-introducing
 * already encountered and fixed bugs.
 */
public class RegressionIT {

    /**
     * This plan revealed an issue with the {@link io.rheem.rheem.core.optimizer.channels.ChannelConversionGraph.ShortestTreeSearcher}.
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
