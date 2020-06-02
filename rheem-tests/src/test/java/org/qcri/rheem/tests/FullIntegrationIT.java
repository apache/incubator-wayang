package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.basic.RheemBasics;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.flink.Flink;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.sqlite3.Sqlite3;
import org.qcri.rheem.tests.platform.MyMadeUpPlatform;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Test the Java integration with Rheem.
 */
public class FullIntegrationIT {

    private Configuration configuration;

    @Before
    public void setUp() throws SQLException, IOException {
        this.configuration = new Configuration();
        File sqlite3dbFile = File.createTempFile("rheem-sqlite3", "db");
        sqlite3dbFile.deleteOnExit();
        this.configuration.setProperty(
                "rheem.sqlite3.jdbc.url",
                "jdbc:sqlite:" + sqlite3dbFile.getAbsolutePath()
        );
        RheemPlans.prepareSqlite3Scenarios(this.configuration);
    }

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        List<String> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin())
                .with(Flink.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        Assert.assertEquals(lines, collector);
    }

    @Test
    public void testReadAndWriteCrossPlatform() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        List<String> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector);
        final Operator sink = rheemPlan.getSinks().stream().findFirst().get();
        sink.addTargetPlatform(Spark.platform());
        final Operator source = sink.getEffectiveOccupant(0).getOwner();
        source.addTargetPlatform(Java.platform());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(RheemPlans.FILE_SOME_LINES_TXT)).collect(Collectors.toList());
        Assert.assertEquals(lines, collector);
    }

    @Test
    public void testReadAndTransformAndWrite() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration1() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        rheemPlan.getSinks().forEach(sink -> sink.addTargetPlatform(MyMadeUpPlatform.getInstance()));

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

    }

    @Test
    public void testMultiSourceAndMultiSink() throws URISyntaxException {
        // Define some input data.
        final List<String> collection1 = Arrays.<String>asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.<String>asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final RheemPlan rheemPlan = RheemPlans.multiSourceMultiSink(collection1, collection2, collector1, collector2);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

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
        final List<String> collection1 = Arrays.<String>asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.<String>asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final RheemPlan rheemPlan = RheemPlans.multiSourceHoleMultiSink(collection1, collection2, collector1, collector2);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

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
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(RheemCollections.asSet(1, 2, 3), RheemCollections.asCollection(collector.get(0), HashSet::new));
    }

    @Test
    public void testIntersect() throws URISyntaxException {
        // Build the RheemPlan.
        List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.intersectSquares(collector, 0, 1, 2, 3, 3, -1, -1, -2, -3, -3, -4);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        Assert.assertEquals(RheemCollections.asSet(1, 4, 9), RheemCollections.asSet(collector));
    }

    @Test
    public void testRepeat() {
        // Build the RheemPlan.
        List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.repeat(collector, 5, 0, 10, 20, 30, 45);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        Assert.assertEquals(5, collector.size());
        Assert.assertEquals(RheemCollections.asSet(5, 15, 25, 35, 50), RheemCollections.asSet(collector));
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
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin())
                .with(RheemBasics.graphPlugin());
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
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());

        // Execute the Rheem plan.
        final Collection<Tuple2<String, Integer>> result = RheemPlans.mapPartitions(rheemContext, 0, 1, 1, 3, 3, 4, 4, 5, 5, 6);

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
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        Assert.assertEquals(1, collector.size());
        Assert.assertEquals(Long.valueOf(5L), collector.get(0));
    }

    @Test
    public void testDiverseScenario1() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario1(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario2() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario3() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario4() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario4(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testSimpleSingleStageLoop() throws URISyntaxException {
        // Build the RheemPlan.
        final Set<Integer> collector = new HashSet<>();
        RheemPlan rheemPlan = RheemPlans.simpleLoop(3, collector, 0, 1, 2);

        rheemPlan.collectTopLevelOperatorByName("source").addTargetPlatform(Spark.platform());
        rheemPlan.collectTopLevelOperatorByName("convergenceSource").addTargetPlatform(Spark.platform());
        rheemPlan.collectTopLevelOperatorByName("loop").addTargetPlatform(Java.platform());
        rheemPlan.collectTopLevelOperatorByName("step").addTargetPlatform(Java.platform());
        rheemPlan.collectTopLevelOperatorByName("counter").addTargetPlatform(Java.platform());
        rheemPlan.collectTopLevelOperatorByName("sink").addTargetPlatform(Spark.platform());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        final HashSet<Integer> expected = new HashSet<>(RheemArrays.asList(RheemArrays.range(0, 24)));
        Assert.assertEquals(expected, collector);
    }

    @Test
    public void testSimpleMultiStageLoop() throws URISyntaxException {
        // Build the RheemPlan.
        final List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.simpleLoop(3, collector, 0, 1, 2);

        rheemPlan.collectTopLevelOperatorByName("source").addTargetPlatform(Spark.platform());
        rheemPlan.collectTopLevelOperatorByName("convergenceSource").addTargetPlatform(Spark.platform());
        rheemPlan.collectTopLevelOperatorByName("loop").addTargetPlatform(Java.platform());
        rheemPlan.collectTopLevelOperatorByName("step").addTargetPlatform(Spark.platform());
        rheemPlan.collectTopLevelOperatorByName("counter").addTargetPlatform(Java.platform());
        rheemPlan.collectTopLevelOperatorByName("sink").addTargetPlatform(Spark.platform());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);

        final HashSet<Integer> expected = new HashSet<>(RheemArrays.asList(RheemArrays.range(0, 24)));
        Assert.assertEquals(expected, RheemCollections.asSet(collector));
    }

    @Test
    public void testSimpleSample() throws URISyntaxException {
        // Build the RheemPlan.
        final List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.simpleSample(3, collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin());

        rheemContext.execute(rheemPlan);
        System.out.println(collector);
    }

    @Test
    public void testCurrentIterationNumber() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        final Collection<Integer> result = RheemPlans.loopWithIterationNumber(rheemContext, 15, 5, -1, 1, 5);
        int expectedOffset = 10;
        Assert.assertEquals(
                RheemCollections.asSet(-1 + expectedOffset, 1 + expectedOffset, 5 + expectedOffset),
                RheemCollections.asSet(result)
        );
    }

    @Test
    public void testCurrentIterationNumberWithTooFewExpectedIterations() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        final Collection<Integer> result = RheemPlans.loopWithIterationNumber(rheemContext, 15, 2, -1, 1, 5);
        int expectedOffset = 10;
        Assert.assertEquals(
                RheemCollections.asSet(-1 + expectedOffset, 1 + expectedOffset, 5 + expectedOffset),
                RheemCollections.asSet(result)
        );
    }

    @Test
    public void testGroupByOperator() {
        final CollectionSource<String> source = new CollectionSource<>(
                Arrays.asList("a", "b", "a", "ab", "aa", "bb"),
                String.class
        );
        final MaterializedGroupByOperator<String, Integer> materializedGroupByOperator =
                new MaterializedGroupByOperator<>(String::length, String.class, Integer.class);
        final MapOperator<Iterable<String>, Integer> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(
                        strings -> (int) StreamSupport.stream(strings.spliterator(), false).count(),
                        DataUnitType.createBasicUnchecked(Iterable.class),
                        DataUnitType.createBasic(Integer.class)
                ),
                DataSetType.createGrouped(String.class),
                DataSetType.createDefault(Integer.class)
        );
        final Collection<Integer> collector = new LinkedList<>();
        final LocalCallbackSink<Integer> sink = LocalCallbackSink.createCollectingSink(collector, Integer.class);

        source.connectTo(0, materializedGroupByOperator, 0);
        materializedGroupByOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, sink, 0);

        RheemContext rheemContext = new RheemContext(configuration)
                .with(Spark.basicPlugin())
                .with(Java.basicPlugin());

        rheemContext.execute(new RheemPlan(sink));
        System.out.println(collector);
    }

    @Test
    public void testSqlite3Scenario1() {
        Collection<Record> collector = new ArrayList<>();
        final RheemPlan rheemPlan = RheemPlans.sqlite3Scenario1(collector);

        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin())
                .with(Sqlite3.plugin());

        rheemContext.execute("SQLite3 scenario 1", rheemPlan);

        Assert.assertEquals(RheemPlans.getSqlite3Customers(), collector);
    }

    @Test
    public void testSqlite3Scenario2() {
        Collection<Record> collector = new ArrayList<>();
        final RheemPlan rheemPlan = RheemPlans.sqlite3Scenario2(collector);

        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin())
                .with(Sqlite3.plugin());

        rheemContext.execute("SQLite3 scenario 2", rheemPlan);

        final List<Record> expected = RheemPlans.getSqlite3Customers().stream()
                .filter(r -> (Integer) r.getField(1) >= 18)
                .collect(Collectors.toList());
        Assert.assertEquals(expected, collector);
    }

    @Test
    public void testSqlite3Scenario3() {
        Collection<Record> collector = new ArrayList<>();
        final RheemPlan rheemPlan = RheemPlans.sqlite3Scenario3(collector);

        RheemContext rheemContext = new RheemContext(configuration)
                .with(Java.basicPlugin())
                .with(Spark.basicPlugin())
                .with(Sqlite3.plugin());

        rheemContext.execute("SQLite3 scenario 3", rheemPlan);

        final List<Record> expected = RheemPlans.getSqlite3Customers().stream()
                .filter(r -> (Integer) r.getField(1) >= 18)
                .map(r -> new Record(new Object[]{r.getField(0)}))
                .collect(Collectors.toList());
        Assert.assertEquals(expected, collector);
    }
}
