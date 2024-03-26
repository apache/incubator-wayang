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

import org.apache.wayang.basic.WayangBasics;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.tests.platform.MyMadeUpPlatform;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test the Spark integration with Wayang.
 */
public class SparkIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Wayang plan.
        List<String> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.readWrite(WayangPlans.FILE_SOME_LINES_TXT, collector);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(WayangPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        Assert.assertEquals(lines, collector);
    }

    @Test
    public void testReadAndTransformAndWrite() throws URISyntaxException {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);
    }

    @Test(expected = WayangException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration1() throws URISyntaxException {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        wayangPlan.getSinks().forEach(sink -> sink.addTargetPlatform(MyMadeUpPlatform.getInstance()));

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());


        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);
    }

    @Test(expected = WayangException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration2() throws URISyntaxException {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        WayangContext wayangContext = new WayangContext();
        // ILLEGAL: This dummy platform is not sufficient to execute the plan.
        wayangContext.register(MyMadeUpPlatform.getInstance());

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);
    }

    @Test(expected = WayangException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration3() throws URISyntaxException {
        // Build a Wayang plan.
        final WayangPlan wayangPlan = WayangPlans.readTransformWrite(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Have Wayang execute the plan.
        final Job job = wayangContext.createJob(null, wayangPlan);
        // ILLEGAL: We blacklist the Spark platform, although we need it.
        job.getConfiguration().getPlatformProvider().addToBlacklist(Spark.platform());
        job.getConfiguration().getPlatformProvider().addToWhitelist(MyMadeUpPlatform.getInstance());
        job.execute();
    }

    @Test
    public void testMultiSourceAndMultiSink() throws URISyntaxException {
        // Define some input data.
        final List<String> collection1 = Arrays.asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final WayangPlan wayangPlan = WayangPlans.multiSourceMultiSink(collection1, collection2, collector1, collector2);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);

        // Check the results in both sinks.
        List<String> expectedOutcome1 = Stream.concat(collection1.stream(), collection2.stream())
                .map(String::toUpperCase)
                .collect(Collectors.toList());
        List<String> expectedOutcome2 = Stream.concat(collection1.stream(), collection2.stream())
                .collect(Collectors.toList());
        Collections.sort(expectedOutcome1);
        Collections.sort(expectedOutcome2);
        Collections.sort(collector1);
        Collections.sort(collector2);
        Assert.assertEquals(expectedOutcome1, collector1);
        Assert.assertEquals(expectedOutcome2, collector2);
    }

    @Test
    public void testMultiSourceAndHoleAndMultiSink() throws URISyntaxException {
        // Define some input data.
        final List<String> collection1 = Arrays.asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final WayangPlan wayangPlan = WayangPlans.multiSourceHoleMultiSink(collection1, collection2, collector1, collector2);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Have Wayang execute the plan.
        wayangContext.execute(wayangPlan);

        // Check the results in both sinks.
        List<String> expectedOutcome = Stream.concat(collection1.stream(), collection2.stream())
                .flatMap(string -> Arrays.asList(string.toLowerCase(), string.toUpperCase()).stream())
                .collect(Collectors.toList());
        Collections.sort(expectedOutcome);
        Collections.sort(collector1);
        Collections.sort(collector2);
        Assert.assertEquals(expectedOutcome, collector1);
        Assert.assertEquals(expectedOutcome, collector2);
    }

    @Test
    public void testGlobalMaterializedGroup() throws URISyntaxException {
        // Build the WayangPlan.
        List<Iterable<Integer>> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.globalMaterializedGroup(collector, 1, 2, 3);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(WayangCollections.asSet(1, 2, 3), WayangCollections.asCollection(collector.get(0), HashSet::new));
    }

    @Test
    public void testIntersect() throws URISyntaxException {
        // Build the WayangPlan.
        List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.intersectSquares(collector, 0, 1, 2, 3, 3, -1, -1, -2, -3, -3, -4);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);

        Assert.assertEquals(WayangCollections.asSet(1, 4, 9), WayangCollections.asSet(collector));
    }

    @Test
    public void testRepeat() {
        // Build the WayangPlan.
        List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.repeat(collector, 5, 0, 10, 20, 30, 45);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext()
                .with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);

        Assert.assertEquals(5, collector.size());
        Assert.assertEquals(WayangCollections.asSet(5, 15, 25, 35, 50), WayangCollections.asSet(collector));
    }

    @Test
    public void testPageRankWithGraphBasic() {
        // Build the WayangPlan.
        List<Tuple2<Long, Long>> edges = Arrays.asList(
                new Tuple2<>(0L, 1L),
                new Tuple2<>(0L, 2L),
                new Tuple2<>(0L, 3L),
                new Tuple2<>(1L, 2L),
                new Tuple2<>(1L, 3L),
                new Tuple2<>(2L, 3L),
                new Tuple2<>(3L, 0L)
        );
        List<Tuple2<Long, Float>> pageRanks = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.pageRank(edges, pageRanks);

        // Execute the plan with a certain backend.
        WayangContext wayangContext = new WayangContext()
                .with(Spark.basicPlugin())
                .with(WayangBasics.graphPlugin());
        wayangContext.execute(wayangPlan);

        // Check the results.
        pageRanks.sort((r1, r2) -> Float.compare(r2.getField1(), r1.getField1()));
        final List<Long> vertexOrder = pageRanks.stream().map(Tuple2::getField0).collect(Collectors.toList());
        Assert.assertEquals(
                Arrays.asList(3L, 0L, 2L, 1L),
                vertexOrder
        );
    }


    @Test
    public void testPageRankWithSparkGraph() {
        // Build the WayangPlan.
        List<Tuple2<Long, Long>> edges = Arrays.asList(
                new Tuple2<>(0L, 1L),
                new Tuple2<>(0L, 2L),
                new Tuple2<>(0L, 3L),
                new Tuple2<>(1L, 2L),
                new Tuple2<>(1L, 3L),
                new Tuple2<>(2L, 3L),
                new Tuple2<>(3L, 0L)
        );
        List<Tuple2<Long, Float>> pageRanks = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.pageRank(edges, pageRanks);

        // Execute the plan with a certain backend.
        WayangContext wayangContext = new WayangContext()
                .with(Spark.basicPlugin())
                .with(Spark.graphPlugin());
        wayangContext.execute(wayangPlan);

        // Check the results.
        pageRanks.sort((r1, r2) -> Float.compare(r2.getField1(), r1.getField1()));
        final List<Long> vertexOrder = pageRanks.stream().map(Tuple2::getField0).collect(Collectors.toList());
        Assert.assertEquals(
                Arrays.asList(3L, 0L, 2L, 1L),
                vertexOrder
        );
    }

    @Test
    public void testMapPartitions() throws URISyntaxException {
        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        // Execute the Wayang plan.
        final Collection<Tuple2<String, Integer>> result = WayangPlans.mapPartitions(wayangContext, 0, 1, 1, 3, 3, 4, 4, 5, 5, 6);

        Assert.assertEquals(
                WayangCollections.asSet(new Tuple2<>("even", 4), new Tuple2<>("odd", 6)),
                WayangCollections.asSet(result)
        );
    }

    @Test
    public void testZipWithId() throws URISyntaxException {
        // Build the WayangPlan.
        List<Long> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.zipWithId(collector, 0, 10, 20, 30, 30);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(Long.valueOf(5L), collector.get(0));
    }

    @Test
    public void testDiverseScenario1() throws URISyntaxException {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario1(WayangPlans.FILE_SOME_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
    }

    @Test
    public void testDiverseScenario2() throws URISyntaxException {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario2(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
    }

    @Test
    public void testDiverseScenario3() throws URISyntaxException {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario3(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Spark backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
    }

    @Test
    public void testDiverseScenario4() throws URISyntaxException {
        // Build the WayangPlan.
        WayangPlan wayangPlan = WayangPlans.diverseScenario4(WayangPlans.FILE_SOME_LINES_TXT, WayangPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
    }

    @Test
    public void testSimpleLoop() throws URISyntaxException {
        // Build the WayangPlan.
        final List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.simpleLoop(3, collector, 0, 1, 2);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
        System.out.println(collector);
    }

    @Test
    public void testSample() throws URISyntaxException {
        // Build the WayangPlan.
        final List<Integer> collector = new LinkedList<>();
        WayangPlan wayangPlan = WayangPlans.simpleSample(3, collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
        System.out.println(collector);
    }

    @Test
    public void testSampleInLoop() {
        final List<Integer> collector = new ArrayList<>();
        WayangPlan wayangPlan = WayangPlans.sampleInLoop(2, 10, collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        Configuration config = new Configuration();
        config.setProperty("spark.executor.cores", "1");
        WayangContext wayangContext = new WayangContext(config)
                .with(Spark.basicPlugin());
        wayangContext.execute(wayangPlan);
        System.out.println(collector);
    }

    @Test
    public void testCurrentIterationNumber() {
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());
        final Collection<Integer> result = WayangPlans.loopWithIterationNumber(wayangContext, 15, 5, -1, 1, 5);
        int expectedOffset = 10;
        Assert.assertEquals(
                WayangCollections.asSet(-1 + expectedOffset, 1 + expectedOffset, 5 + expectedOffset),
                WayangCollections.asSet(result)
        );
    }

    @Test
    public void testCurrentIterationNumberWithTooFewExpectedIterations() {
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());
        final Collection<Integer> result = WayangPlans.loopWithIterationNumber(wayangContext, 15, 2, -1, 1, 5);
        int expectedOffset = 10;
        Assert.assertEquals(
                WayangCollections.asSet(-1 + expectedOffset, 1 + expectedOffset, 5 + expectedOffset),
                WayangCollections.asSet(result)
        );
    }

    @Test
    public void testBroadcasts() {
        Collection<Integer> broadcastedValues = Arrays.asList(1, 2, 3, 4);
        Collection<Integer> mainValues = Arrays.asList(2, 4, 6, 2);
        List<Integer> collectedValues = new ArrayList<>();
        List<Integer> expectedValues = Arrays.asList(2, 2, 4);

        final DataSetType<Integer> integerDataSetType = DataSetType.createDefault(Integer.class);
        CollectionSource<Integer> broadcastSource = new CollectionSource<>(broadcastedValues, integerDataSetType);
        CollectionSource<Integer> mainSource = new CollectionSource<>(mainValues, integerDataSetType);
        FilterOperator<Integer> semijoin = new FilterOperator<>(integerDataSetType, new SemijoinFunction());
        final LocalCallbackSink<Integer> collectingSink =
                LocalCallbackSink.createCollectingSink(collectedValues, integerDataSetType);

        mainSource.connectTo(0, semijoin, 0);
        broadcastSource.broadcastTo(0, semijoin, "allowed values");
        semijoin.connectTo(0, collectingSink, 0);

        WayangPlan wayangPlan = new WayangPlan(collectingSink);

        // Instantiate Wayang and activate the Java backend.
        WayangContext wayangContext = new WayangContext().with(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);

        Collections.sort(collectedValues);
        Assert.assertEquals(expectedValues, collectedValues);
    }

    @Test
    public void testKMeans() {
        CollectionSource<double[]> collectionSource = new CollectionSource<>(
                Arrays.asList(
                        new double[]{1, 2, 3},
                        new double[]{-1, -2, -3},
                        new double[]{2, 4, 6}),
                double[].class
        );
        collectionSource.addTargetPlatform(Java.platform());
        collectionSource.addTargetPlatform(Spark.platform());

        KMeansOperator kMeansOperator = new KMeansOperator(2);

        PredictOperator<double[], Integer> transformOperator = PredictOperators.kMeans();

        // Write results to a sink.
        List<Integer> results = new ArrayList<>();
        LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(results, DataSetType.createDefault(Integer.class));

        // Build Wayang plan by connecting operators
        collectionSource.connectTo(0, kMeansOperator, 0);
        kMeansOperator.connectTo(0, transformOperator, 0);
        collectionSource.connectTo(0, transformOperator, 1);
        transformOperator.connectTo(0, sink, 0);
        WayangPlan wayangPlan = new WayangPlan(sink);

        // Have Wayang execute the plan.
        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        wayangContext.register(Spark.mlPlugin());
        wayangContext.execute(wayangPlan);

        // Verify the outcome.
        Assert.assertEquals(3, results.size());
        Assert.assertEquals(
                results.get(0),
                results.get(2)
        );
    }

    private static class SemijoinFunction implements PredicateDescriptor.ExtendedSerializablePredicate<Integer> {

        private Set<Integer> allowedInts;

        @Override
        public void open(ExecutionContext ctx) {
            this.allowedInts = new HashSet<>(ctx.getBroadcast("allowed values"));
        }

        @Override
        public boolean test(Integer integer) {
            return this.allowedInts.contains(integer);
        }
    }
}
