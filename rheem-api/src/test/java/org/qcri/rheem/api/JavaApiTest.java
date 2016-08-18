package org.qcri.rheem.api;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.core.util.fs.LocalFileSystem;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.java.operators.JavaMapOperator;
import org.qcri.rheem.spark.Spark;
import org.qcri.rheem.sqlite3.Sqlite3;
import org.qcri.rheem.sqlite3.operators.Sqlite3TableSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test suite for the Java API.
 */
public class JavaApiTest {

    private Configuration sqlite3Configuration;

    @Before
    public void setUp() throws SQLException, IOException {
        // Generate test data.
        this.sqlite3Configuration = new Configuration();
        File sqlite3dbFile = File.createTempFile("rheem-sqlite3", "db");
        sqlite3dbFile.deleteOnExit();
        this.sqlite3Configuration.setProperty("rheem.sqlite3.jdbc.url", "jdbc:sqlite:" + sqlite3dbFile.getAbsolutePath());
        try (Connection connection = Sqlite3.platform().createDatabaseDescriptor(this.sqlite3Configuration).createJdbcConnection()) {
            Statement statement = connection.createStatement();
            statement.addBatch("DROP TABLE IF EXISTS customer;");
            statement.addBatch("CREATE TABLE customer (name TEXT, age INT);");
            statement.addBatch("INSERT INTO customer VALUES ('John', 20)");
            statement.addBatch("INSERT INTO customer VALUES ('Timmy', 16)");
            statement.addBatch("INSERT INTO customer VALUES ('Evelyn', 35)");
            statement.executeBatch();
        }
    }

    @Test
    public void testMapReduce() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .globalReduce((a, b) -> a + b).withName("sum")
                .collect("testMapReduce()");

        Assert.assertEquals(RheemCollections.asSet(1 + 4 + 9 + 16), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testMapReduceBy() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .reduceBy(i -> i & 1, (a, b) -> a + b).withName("sum")
                .collect("testMapReduceBy()");

        Assert.assertEquals(RheemCollections.asSet(4 + 16, 1 + 9), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testBroadcast2() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(rheemContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        List<Integer> offsetCollection = Collections.singletonList(-2);

        LoadCollectionDataQuantaBuilder<Integer> offsetDataQuanta = javaPlanBuilder
                .loadCollection(offsetCollection)
                .withName("load offset");

        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(new AddOffset("offset")).withName("add offset").withBroadcast(offsetDataQuanta, "offset")
                .collect("testBroadcast()");

        Assert.assertEquals(RheemCollections.asSet(-2, -1, 0, 1, 2), RheemCollections.asSet(outputCollection));
    }

    @Test
    public void testCustomOperatorShortCut() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        final List<Integer> inputValues = RheemArrays.asList(0, 1, 2, 3);

        // Build and execute a Rheem plan.
        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .<Integer>customOperator(new JavaMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new TransformationDescriptor<>(
                                i -> i + 2,
                                Integer.class, Integer.class
                        )
                )).withName("Add 2")
                .collect("testCustomOperatorShortCut()");

        // Check the outcome.
        final List<Integer> expectedOutputValues = RheemArrays.asList(2, 3, 4, 5);
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testWordCount() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Rheem plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceBy(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect("testWordCount()");

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = RheemCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testWordCountOnSparkAndJava() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin()).with(Spark.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Rheem plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceBy(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect("testWordCount()");

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = RheemCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(RheemCollections.asSet(expectedOutputValues), RheemCollections.asSet(outputValues));
    }

    @Test
    public void testDoWhile() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = RheemArrays.asList(1, 2);

        // Build and execute a word count RheemPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .doWhile(
                        values -> values.stream().mapToInt(i -> i).sum() > 100,
                        start -> {
                            final GlobalReduceDataQuantaBuilder<Integer> sum =
                                    start.globalReduce((a, b) -> a + b).withName("sum");
                            return new Tuple<>(
                                    start.union(sum).withName("Old+new"),
                                    sum.map(x -> x).withName("Identity (hotfix)")
                            );
                        }
                ).withName("While <= 100")
                .collect("testDoWhile()");

        Set<Integer> expectedValues = RheemCollections.asSet(1, 2, 3, 6, 12, 24, 48, 96, 192);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    private static class AddOffset implements FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer> {

        private final String broadcastName;

        private int offset;

        public AddOffset(String broadcastName) {
            this.broadcastName = broadcastName;
        }

        @Override
        public void open(ExecutionContext ctx) {
            this.offset = RheemCollections.getSingle(ctx.<Integer>getBroadcast(this.broadcastName));
        }

        @Override
        public Integer apply(Integer input) {
            return input + this.offset;
        }
    }

    @Test
    public void testRepeat() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = RheemArrays.asList(1, 2);

        // Build and execute a word count RheemPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(rheemContext)
                .loadCollection(inputValues).withName("Load input values")
                .repeat(3, start -> start
                        .globalReduce((a, b) -> a * b).withName("Multiply")
                        .flatMap(v -> Arrays.asList(v, v + 1)).withName("Duplicate").withOutputClass(Integer.class)
                ).withName("Repeat 3x")
                .collect("testRepeat()");

        Set<Integer> expectedValues = RheemCollections.asSet(42, 43);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    private static class SelectWords implements PredicateDescriptor.ExtendedSerializablePredicate<String> {

        private final String broadcastName;

        private Collection<Character> selectors;

        public SelectWords(String broadcastName) {
            this.broadcastName = broadcastName;
        }

        @Override
        public void open(ExecutionContext ctx) {
            this.selectors = ctx.getBroadcast(this.broadcastName);
        }

        @Override
        public boolean test(String word) {
            return this.selectors.stream().anyMatch(c -> word.indexOf(c) >= 0);
        }
    }

    @Test
    public void testBroadcast() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        final List<String> inputValues = Arrays.asList("Hello", "World", "Hi", "Mars");
        final List<Character> selectors = Arrays.asList('o', 'l');

        // Execute the job.
        final DataQuantaBuilder<?, Character> selectorsDataSet = builder.loadCollection(selectors).withName("Load selectors");
        final Collection<String> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .filter(new SelectWords("selectors")).withName("Filter words")
                .withBroadcast(selectorsDataSet, "selectors")
                .collect("testBroadcast()");

        // Verify the outcome.
        Set<String> expectedValues = RheemCollections.asSet("Hello", "World");
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    @Test
    public void testGroupBy() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 7, 8, 9, 10);

        // Execute the job.
        final Collection<Double> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .groupBy(i -> i % 2).withName("group odd and even")
                .map(group -> {
                    List<Integer> sortedGroup = StreamSupport.stream(group.spliterator(), false)
                            .sorted()
                            .collect(Collectors.toList());
                    int sizeDivTwo = sortedGroup.size() / 2;
                    return sortedGroup.size() % 2 == 0 ?
                            (sortedGroup.get(sizeDivTwo - 1) + sortedGroup.get(sizeDivTwo)) / 2d :
                            (double) sortedGroup.get(sizeDivTwo);
                })
                .collect("testGroupBy()");

        // Verify the outcome.
        Set<Double> expectedValues = RheemCollections.asSet(5d, 6d);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    @Test
    public void testJoin() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        final List<Tuple2<String, Integer>> inputValues1 = Arrays.asList(
                new Tuple2<>("Water", 0),
                new Tuple2<>("Tonic", 5),
                new Tuple2<>("Juice", 10)
        );
        final List<Tuple2<String, String>> inputValues2 = Arrays.asList(
                new Tuple2<>("Apple juice", "Juice"),
                new Tuple2<>("Tap water", "Water"),
                new Tuple2<>("Orange juice", "Juice")
        );

        // Execute the job.
        final LoadCollectionDataQuantaBuilder<Tuple2<String, Integer>> dataQuanta1 = builder.loadCollection(inputValues1);
        final LoadCollectionDataQuantaBuilder<Tuple2<String, String>> dataQuanta2 = builder.loadCollection(inputValues2);
        final Collection<Tuple2<String, Integer>> outputValues = dataQuanta1
                .join(Tuple2::getField0, dataQuanta2, Tuple2::getField1)
                .map(joinTuple -> new Tuple2<>(joinTuple.getField1().getField0(), joinTuple.getField0().getField1()))
                .collect("testJoin()");

        // Verify the outcome.
        Set<Tuple2<String, Integer>> expectedValues = RheemCollections.asSet(
                new Tuple2<>("Apple juice", 10),
                new Tuple2<>("Orange juice", 10),
                new Tuple2<>("Tap water", 0)
        );
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    @Test
    public void testIntersect() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        final List<Integer> inputValues1 = Arrays.asList(1, 2, 3, 4, 5, 7, 8, 9, 10);
        final List<Integer> inputValues2 = Arrays.asList(0, 2, 3, 3, 4, 5, 7, 8, 9, 11);

        // Execute the job.
        final LoadCollectionDataQuantaBuilder<Integer> dataQuanta1 = builder.loadCollection(inputValues1);
        final LoadCollectionDataQuantaBuilder<Integer> dataQuanta2 = builder.loadCollection(inputValues2);
        final Collection<Integer> outputValues = dataQuanta1.intersect(dataQuanta2).collect("testIntersect()");

        // Verify the outcome.
        Set<Integer> expectedValues = RheemCollections.asSet(2, 3, 4, 5, 7, 8, 9);
        Assert.assertEquals(expectedValues, RheemCollections.asSet(outputValues));
    }

    @Test
    public void testPageRank() {
        // Set up RheemContext.
        RheemContext rheemContext = new RheemContext()
                .with(Java.basicPlugin())
                .with(Java.graphPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Create a test graph.
        Collection<Tuple2<Long, Long>> edges = Arrays.asList(
                new Tuple2<>(0L, 1L),
                new Tuple2<>(0L, 2L),
                new Tuple2<>(0L, 3L),
                new Tuple2<>(1L, 0L),
                new Tuple2<>(2L, 1L),
                new Tuple2<>(3L, 2L),
                new Tuple2<>(3L, 1L)
        );

        // Execute the job.
        Collection<Tuple2<Long, Float>> pageRanks = builder.loadCollection(edges).asEdges()
                .pageRank(20)
                .collect("testPageRank()");
        List<Tuple2<Long, Float>> sortedPageRanks = new ArrayList<>(pageRanks);
        sortedPageRanks.sort((pr1, pr2) -> Float.compare(pr2.field1, pr1.field1));

        System.out.println(sortedPageRanks);
        Assert.assertEquals(1L, sortedPageRanks.get(0).field0.longValue());
        Assert.assertEquals(0L, sortedPageRanks.get(1).field0.longValue());
        Assert.assertEquals(2L, sortedPageRanks.get(2).field0.longValue());
        Assert.assertEquals(3L, sortedPageRanks.get(3).field0.longValue());
    }

    @Test
    public void testZipWithId() {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        List<Integer> inputValues = new ArrayList<>(42 * 100);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 42; j++) {
                inputValues.add(i);
            }
        }

        // Execute the job.
        Collection<Tuple2<Integer, Integer>> outputValues = builder.loadCollection(inputValues)
                .zipWithId()
                .groupBy(Tuple2::getField1)
                .map(group -> {
                    int distinctIds = (int) StreamSupport.stream(group.spliterator(), false)
                            .map(Tuple2::getField0)
                            .distinct()
                            .count();
                    return new Tuple2<>(distinctIds, 1);
                })
                .reduceBy(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .collect("testZipWithId()");

        // Check the output.
        Set<Tuple2<Integer, Integer>> expectedOutput = Collections.singleton(new Tuple2<>(42, 100));
        Assert.assertEquals(expectedOutput, RheemCollections.asSet(outputValues));
    }

    @Test
    public void testWriteTextFile() throws IOException, URISyntaxException {
        RheemContext rheemContext = new RheemContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemContext);

        // Generate test data.
        List<Double> inputValues = Arrays.asList(0d, 1 / 3d, 2 / 3d, 1d, 4 / 3d, 5 / 3d);

        // Execute the job.
        File tempDir = LocalFileSystem.findTempDir();
        String targetUrl = LocalFileSystem.toURL(new File(tempDir, "testWriteTextFile.txt"));

        builder
                .loadCollection(inputValues)
                .writeTextFile(targetUrl, d -> String.format("%.2f", d), "testWriteTextFile()");

        // Check the output.
        Set<String> actualLines = Files.lines(Paths.get(new URI(targetUrl))).collect(Collectors.toSet());
        Set<String> expectedLines = inputValues.stream().map(d -> String.format("%.2f", d)).collect(Collectors.toSet());
        Assert.assertEquals(expectedLines, actualLines);
    }

    @Test
    public void testSqlOnJava() throws IOException, SQLException {
        // Execute job.
        final RheemContext rheemCtx = new RheemContext(this.sqlite3Configuration)
                .with(Java.basicPlugin())
                .with(Sqlite3.plugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemCtx);
        final Collection<String> outputValues = builder
                .readTable(new Sqlite3TableSource("customer", "name", "age"))
                .filter(r -> (Integer) r.getField(1) >= 18).withSqlUdf("age >= 18").withTargetPlatform(Java.platform())
                .asRecords().projectRecords(new String[]{"name"})
                .map(record -> (String) record.getField(0))
                .collect("testSqlOnJava()");

        // Test the outcome.
        Assert.assertEquals(
                RheemCollections.asSet("John", "Evelyn"),
                RheemCollections.asSet(outputValues)
        );
    }

    @Test
    public void testSqlOnSqlite3() throws IOException, SQLException {
        // Execute job.
        final RheemContext rheemCtx = new RheemContext(this.sqlite3Configuration)
                .with(Java.basicPlugin())
                .with(Sqlite3.plugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemCtx);
        final Collection<String> outputValues = builder
                .readTable(new Sqlite3TableSource("customer", "name", "age"))
                .filter(r -> (Integer) r.getField(1) >= 18).withSqlUdf("age >= 18")
                .asRecords().projectRecords(new String[]{"name"}).withTargetPlatform(Sqlite3.platform())
                .map(record -> (String) record.getField(0))
                .collect("testSqlOnSqlite3()");

        // Test the outcome.
        Assert.assertEquals(
                RheemCollections.asSet("John", "Evelyn"),
                RheemCollections.asSet(outputValues)
        );
    }

}
