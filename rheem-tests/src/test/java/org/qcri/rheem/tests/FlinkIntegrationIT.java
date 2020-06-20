package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.tests.platform.MyMadeUpPlatform;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test the Spark integration with Rheem.
 */
public class FlinkIntegrationIT {

    private static final String JAVA  = "JAVA";
    private static final String FLINK = "JAVA";
    private static final String BOTH  = "BOTH";

    private RheemContext makeContext(String plugin){
        RheemContext rheemContext = new RheemContext();
        if(plugin == JAVA || plugin == BOTH)
            rheemContext.with(Java.basicPlugin());
        if(plugin == FLINK || plugin == BOTH)
            rheemContext.with(Flink.basicPlugin());
        return rheemContext;
    }

    private void makeAndRun(RheemPlan plan, String plugin){
        RheemContext rheemContext = this.makeContext(plugin);
        rheemContext.execute(plan);
    }

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        List<String> collector = new LinkedList<>();

        makeAndRun(RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector), FLINK);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        Assert.assertEquals(lines, collector);
    }


    @Test
    public void testReadAndTransformAndWrite() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        makeAndRun(rheemPlan, FLINK);
    }

    @Test
    public void testCartesianOperator() throws IOException {

        List<Tuple2<String, String>> collector = new ArrayList<>();
        final RheemPlan rheemPlan = RheemPlansOperators.cartesian(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        // Run in java for test result
        List<Tuple2<String, String>> collectorJava = new ArrayList<>();
        final RheemPlan rheemPlanJava = RheemPlansOperators.cartesian(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testCoGroupOperator() throws IOException {
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        final RheemPlan rheemPlan = RheemPlansOperators.coGroup(RheemPlans.FILE_WITH_KEY_1, RheemPlans.FILE_WITH_KEY_2, collector);
        makeAndRun(rheemPlan, FLINK);


        // Run in java for test result
        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        final RheemPlan rheemPlanJava = RheemPlansOperators.coGroup(RheemPlans.FILE_WITH_KEY_1, RheemPlans.FILE_WITH_KEY_2, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testCollectionSource(){
        List<String> input = makeList();
        List<String> collector = new ArrayList<>();

        RheemPlan rheemplan = RheemPlansOperators.collectionSourceOperator(input, collector);
        makeAndRun(rheemplan, FLINK);

        Assert.assertEquals(input, collector);
    }

    @Test
    public void testCountOperator(){
        List<String> input = makeList();
        List<Long> collector = new ArrayList<>();

        RheemPlan rheemPlan = RheemPlansOperators.count(input, collector);
        makeAndRun(rheemPlan, FLINK);

        Assert.assertTrue(input.size() == collector.get(0));
    }

    @Test
    public void testDistinctOperator(){
        List<String> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.distinct(RheemPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.distinct(RheemPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava.stream().sorted().toArray(), collector.stream().sorted().toArray());
    }

    @Test
    public void testFilterOperator(){
        List<String> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.filter(RheemPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.filter(RheemPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testFlapMapOperator(){
        List<String> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.flatMap(RheemPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.flatMap(RheemPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testJoinOperator(){
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.join(RheemPlans.FILE_WITH_KEY_1, RheemPlans.FILE_WITH_KEY_2, collector);
        makeAndRun(rheemPlan, FLINK);

        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.join(RheemPlans.FILE_WITH_KEY_1, RheemPlans.FILE_WITH_KEY_2, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }


    @Test
    public void testReduceByOperator(){
        List<Tuple2<?, ?>> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.reduceBy(RheemPlans.FILE_WITH_KEY_1, collector);
        makeAndRun(rheemPlan, FLINK);

        List<Tuple2<?, ?>> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.reduceBy(RheemPlans.FILE_WITH_KEY_1, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testSortOperator(){
        List<String> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.sort(RheemPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.sort(RheemPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testTextFileSink() throws IOException {

        File temp = File.createTempFile("tempfile", ".tmp");

        temp.delete();

        RheemPlan rheemPlan = RheemPlansOperators.textFileSink(RheemPlans.FILE_SOME_LINES_TXT, temp.toURI());
        makeAndRun(rheemPlan, FLINK);

        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        final List<String> linesFlink = Files.lines(Paths.get(temp.toURI())).collect(Collectors.toList());


        Assert.assertEquals(lines, linesFlink);

        temp.delete();

    }

    @Test
    public void testUnionOperator(){
        List<String> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.union(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<String> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.union(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    @Test
    public void testZipWithIdOperator(){
        List<Tuple2<Long, String>> collector = new ArrayList<>();
        RheemPlan rheemPlan = RheemPlansOperators.zipWithId(RheemPlans.FILE_SOME_LINES_TXT, collector);
        makeAndRun(rheemPlan, FLINK);

        List<Tuple2<Long, String>> collectorJava = new ArrayList<>();
        RheemPlan rheemPlanJava = RheemPlansOperators.zipWithId(RheemPlans.FILE_SOME_LINES_TXT, collectorJava);
        makeAndRun(rheemPlanJava, JAVA);

        Assert.assertEquals(collectorJava, collector);
    }

    private List<String> makeList(){
        return Arrays.asList(
                "word1",
                "word2",
                "word3",
                "word4",
                "word5",
                "word6",
                "word7",
                "word8",
                "word9",
                "word10"
        );
    }




    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration1() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        rheemPlan.getSinks().forEach(sink -> sink.addTargetPlatform(MyMadeUpPlatform.getInstance()));

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = makeContext(FLINK);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration2() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);

        RheemContext rheemContext = new RheemContext();
        // ILLEGAL: This dummy platform is not sufficient to execute the plan.
        rheemContext.register(MyMadeUpPlatform.getInstance());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration3() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = makeContext(FLINK);

        // Have Rheem execute the plan.
        final Job job = rheemContext.createJob(null, rheemPlan);
        // ILLEGAL: We blacklist the Spark platform, although we need it.
        job.getConfiguration().getPlatformProvider().addToBlacklist(Flink.platform());
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
        final RheemPlan rheemPlan = RheemPlans.multiSourceMultiSink(collection1, collection2, collector1, collector2);

        makeAndRun(rheemPlan, FLINK);

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
        final RheemPlan rheemPlan = RheemPlans.multiSourceHoleMultiSink(collection1, collection2, collector1, collector2);


        makeAndRun(rheemPlan, FLINK);

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
        // Build the RheemPlan.
        List<Iterable<Integer>> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.globalMaterializedGroup(collector, 1, 2, 3);

        // Instantiate Rheem and activate the Java backend.
        makeAndRun(rheemPlan, FLINK);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(RheemCollections.asSet(1, 2, 3), RheemCollections.asCollection(collector.get(0), HashSet::new));
    }

    @Test
    public void testIntersect() throws URISyntaxException {
        // Build the RheemPlan.
        List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.intersectSquares(collector, 0, 1, 2, 3, 3, -1, -1, -2, -3, -3, -4);

        // Instantiate Rheem and activate the Java backend.
        makeAndRun(rheemPlan, FLINK);

        Assert.assertEquals(RheemCollections.asSet(1, 4, 9), RheemCollections.asSet(collector));
    }

    @Test
    public void testPageRankWithGraphBasic() {
        // Build the RheemPlan.
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
        RheemPlan rheemPlan = RheemPlans.pageRank(edges, pageRanks);

        // Execute the plan with a certain backend.
        RheemContext rheemContext = new RheemContext()
                .with(Flink.basicPlugin());
                //.with(RheemBasics.graphPlugin());
        rheemContext.execute(rheemPlan);


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
        // Execute the Rheem plan.
        final Collection<Tuple2<String, Integer>> result = new ArrayList<>();

        RheemPlan rheemPlan = RheemPlansOperators.mapPartitions(result, 0, 1, 1, 3, 3, 4, 4, 5, 5, 6);

        makeAndRun(rheemPlan, FLINK);

        Assert.assertEquals(
                RheemCollections.asSet(new Tuple2<>("even", 4), new Tuple2<>("odd", 6)),
                RheemCollections.asSet(result)
        );
    }

    @Test
    public void testZipWithId() throws URISyntaxException {
        // Build the RheemPlan.
        List<Long> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.zipWithId(collector, 0, 10, 20, 30, 30);

        // Instantiate Rheem and activate the Java backend.
        makeAndRun(rheemPlan, FLINK);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(Long.valueOf(5L), collector.get(0));
    }

    @Test
    public void testDiverseScenario1() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario1(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        makeAndRun(rheemPlan, FLINK);
    }

    @Test
    public void testDiverseScenario2() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        makeAndRun(rheemPlan, FLINK);
    }

    @Test
    public void testDiverseScenario3() throws URISyntaxException {
        // Build the RheemPlan.
        //TODO: need implement the loop for running this test
        //RheemPlan rheemPlan = RheemPlans.diverseScenario3(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        //makeAndRun(rheemPlan, FLINK);
    }

    @Test
    public void testDiverseScenario4() throws URISyntaxException {
        // Build the RheemPlan.
        //TODO: need implement the loop for running this test
        //RheemPlan rheemPlan = RheemPlans.diverseScenario4(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        //makeAndRun(rheemPlan, FLINK);
    }


    @Test
    public void testSample() throws URISyntaxException {
        // Build the RheemPlan.
        final List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.simpleSample(3, collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Instantiate Rheem and activate the Java backend.
        makeAndRun(rheemPlan, FLINK);

        System.out.println(collector);
    }


}
