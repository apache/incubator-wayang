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

package org.apache.wayang.api;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.ExecutionContext;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangArrays;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.fs.LocalFileSystem;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.operators.JavaMapOperator;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.sqlite3.Sqlite3;
import org.apache.wayang.sqlite3.operators.Sqlite3TableSource;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
        File sqlite3dbFile = File.createTempFile("wayang-sqlite3", "db");
        sqlite3dbFile.deleteOnExit();
        this.sqlite3Configuration.setProperty("wayang.sqlite3.jdbc.url", "jdbc:sqlite:" + sqlite3dbFile.getAbsolutePath());
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
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .reduce((a, b) -> a + b).withName("sum")
                .collect();

        Assert.assertEquals(WayangCollections.asSet(1 + 4 + 9 + 16), WayangCollections.asSet(outputCollection));
    }

    @Test
    public void testMapReduceBy() {
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(i -> i * i).withName("square")
                .reduceByKey(i -> i & 1, (a, b) -> a + b).withName("sum")
                .collect();

        Assert.assertEquals(WayangCollections.asSet(4 + 16, 1 + 9), WayangCollections.asSet(outputCollection));
    }

    @Test
    public void testBroadcast2() {
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext);

        List<Integer> inputCollection = Arrays.asList(0, 1, 2, 3, 4);
        List<Integer> offsetCollection = Collections.singletonList(-2);

        LoadCollectionDataQuantaBuilder<Integer> offsetDataQuanta = javaPlanBuilder
                .loadCollection(offsetCollection)
                .withName("load offset");

        Collection<Integer> outputCollection = javaPlanBuilder
                .loadCollection(inputCollection).withName("load numbers")
                .map(new AddOffset("offset")).withName("add offset").withBroadcast(offsetDataQuanta, "offset")
                .collect();

        Assert.assertEquals(WayangCollections.asSet(-2, -1, 0, 1, 2), WayangCollections.asSet(outputCollection));
    }

    @Test
    public void testCustomOperatorShortCut() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());

        final List<Integer> inputValues = WayangArrays.asList(0, 1, 2, 3);

        // Build and execute a Wayang plan.
        final Collection<Integer> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .<Integer>customOperator(new JavaMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new TransformationDescriptor<>(
                                i -> i + 2,
                                Integer.class, Integer.class
                        )
                )).withName("Add 2")
                .collect();

        // Check the outcome.
        final List<Integer> expectedOutputValues = WayangArrays.asList(2, 3, 4, 5);
        Assert.assertEquals(WayangCollections.asSet(expectedOutputValues), WayangCollections.asSet(outputValues));
    }

    @Test
    public void testWordCount() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Wayang plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect();

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = WayangCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(WayangCollections.asSet(expectedOutputValues), WayangCollections.asSet(outputValues));
    }

    @Test
    public void testWordCountOnSparkAndJava() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin()).with(Spark.basicPlugin());

        final List<String> inputValues = Arrays.asList("Big data is big.", "Is data big data?");

        // Build and execute a Wayang plan.
        final Collection<Tuple2<String, Integer>> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .flatMap(line -> Arrays.asList(line.split("\\s+"))).withName("Split words")
                .map(token -> token.replaceAll("\\W+", "").toLowerCase()).withName("To lower case")
                .map(word -> new Tuple2<>(word, 1)).withName("Attach counter")
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1)).withName("Sum counters")
                .collect();

        // Check the outcome.
        final Set<Tuple2<String, Integer>> expectedOutputValues = WayangCollections.asSet(
                new Tuple2<>("big", 3),
                new Tuple2<>("is", 2),
                new Tuple2<>("data", 3)
        );
        Assert.assertEquals(WayangCollections.asSet(expectedOutputValues), WayangCollections.asSet(outputValues));
    }

    @Test
    public void testSample() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin()).with(Spark.basicPlugin());

        // Create some input values.
        final List<Integer> inputValues = WayangArrays.asList(WayangArrays.range(100));

        // Build and execute a Wayang plan.
        final Collection<Integer> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .sample(10).withName("Sample")
                .collect();

        // Check the outcome.
        Assert.assertEquals(10, outputValues.size());
        Assert.assertEquals(10, WayangCollections.asSet(outputValues).size());

    }

    @Test
    public void testDoWhile() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = WayangArrays.asList(1, 2);

        // Build and execute a word count WayangPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .doWhile(
                        values -> values.stream().mapToInt(i -> i).sum() > 100,
                        start -> {
                            final GlobalReduceDataQuantaBuilder<Integer> sum =
                                    start.reduce((a, b) -> a + b).withName("sum");
                            return new Tuple<>(
                                    start.union(sum).withName("Old+new"),
                                    sum
                            );
                        }
                ).withConditionClass(Integer.class).withName("While <= 100")
                .collect();

        Set<Integer> expectedValues = WayangCollections.asSet(1, 2, 3, 6, 12, 24, 48, 96, 192);
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    private static class AddOffset implements FunctionDescriptor.ExtendedSerializableFunction<Integer, Integer> {

        private final String broadcastName;

        private int offset;

        public AddOffset(String broadcastName) {
            this.broadcastName = broadcastName;
        }

        @Override
        public void open(ExecutionContext ctx) {
            this.offset = WayangCollections.getSingle(ctx.<Integer>getBroadcast(this.broadcastName));
        }

        @Override
        public Integer apply(Integer input) {
            return input + this.offset;
        }
    }

    @Test
    public void testRepeat() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());

        // Generate test data.
        final List<Integer> inputValues = WayangArrays.asList(1, 2);

        // Build and execute a word count WayangPlan.

        final Collection<Integer> outputValues = new JavaPlanBuilder(wayangContext)
                .loadCollection(inputValues).withName("Load input values")
                .repeat(3, start -> start
                        .reduce((a, b) -> a * b).withName("Multiply")
                        .flatMap(v -> Arrays.asList(v, v + 1)).withName("Duplicate").withOutputClass(Integer.class)
                ).withName("Repeat 3x")
                .collect();

        Set<Integer> expectedValues = WayangCollections.asSet(42, 43);
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
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
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<String> inputValues = Arrays.asList("Hello", "World", "Hi", "Mars");
        final List<Character> selectors = Arrays.asList('o', 'l');

        // Execute the job.
        final DataQuantaBuilder<?, Character> selectorsDataSet = builder.loadCollection(selectors).withName("Load selectors");
        final Collection<String> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .filter(new SelectWords("selectors")).withName("Filter words")
                .withBroadcast(selectorsDataSet, "selectors")
                .collect();

        // Verify the outcome.
        Set<String> expectedValues = WayangCollections.asSet("Hello", "World");
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testGroupBy() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues = Arrays.asList(1, 2, 3, 4, 5, 7, 8, 9, 10);

        // Execute the job.
        final Collection<Double> outputValues = builder
                .loadCollection(inputValues).withName("Load input values")
                .groupByKey(i -> i % 2).withName("group odd and even")
                .map(group -> {
                    List<Integer> sortedGroup = StreamSupport.stream(group.spliterator(), false)
                            .sorted()
                            .collect(Collectors.toList());
                    int sizeDivTwo = sortedGroup.size() / 2;
                    return sortedGroup.size() % 2 == 0 ?
                            (sortedGroup.get(sizeDivTwo - 1) + sortedGroup.get(sizeDivTwo)) / 2d :
                            (double) sortedGroup.get(sizeDivTwo);
                })
                .collect();

        // Verify the outcome.
        Set<Double> expectedValues = WayangCollections.asSet(5d, 6d);
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testJoin() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

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
                .collect();

        // Verify the outcome.
        Set<Tuple2<String, Integer>> expectedValues = WayangCollections.asSet(
                new Tuple2<>("Apple juice", 10),
                new Tuple2<>("Orange juice", 10),
                new Tuple2<>("Tap water", 0)
        );
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testJoinAndAssemble() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

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
        final Collection<Tuple2<String, Integer>> outputValues = dataQuanta1.keyBy(Tuple2::getField0)
                .join(dataQuanta2.keyBy(Tuple2::getField1))
                .assemble((val1, val2) -> new Tuple2<>(val2.getField0(), val1.getField1()))
                .collect();

        // Verify the outcome.
        Set<Tuple2<String, Integer>> expectedValues = WayangCollections.asSet(
                new Tuple2<>("Apple juice", 10),
                new Tuple2<>("Orange juice", 10),
                new Tuple2<>("Tap water", 0)
        );
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testCoGroup() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Tuple2<String, Integer>> inputValues1 = Arrays.asList(
                new Tuple2<>("Water", 0),
                new Tuple2<>("Cola", 5),
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
        final Collection<Tuple2<Set<Tuple2<String, Integer>>, Set<Tuple2<String, String>>>> outputValues = dataQuanta1
                .coGroup(Tuple2::getField0, dataQuanta2, Tuple2::getField1)
                .map(joinTuple -> new Tuple2<>(
                        WayangCollections.asSet(joinTuple.getField0()),
                        WayangCollections.asSet(joinTuple.getField1())
                ))
                .collect();

        // Verify the outcome.
        Set<Tuple2<Set<Tuple2<String, Integer>>, Set<Tuple2<String, String>>>> expectedValues = WayangCollections.asSet(
                new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Water", 0)),
                        WayangCollections.asSet(new Tuple2<>("Tap water", "Water"))
                ),
                new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Cola", 5)),
                        WayangCollections.asSet()
                ), new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Juice", 10)),
                        WayangCollections.asSet(new Tuple2<>("Apple juice", "Juice"), new Tuple2<>("Orange juice", "Juice"))
                )
        );
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testCoGroupViaKeyBy() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin()).with(Spark.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Tuple2<String, Integer>> inputValues1 = Arrays.asList(
                new Tuple2<>("Water", 0),
                new Tuple2<>("Cola", 5),
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
        final Collection<Tuple2<Set<Tuple2<String, Integer>>, Set<Tuple2<String, String>>>> outputValues =
                dataQuanta1.keyBy(Tuple2::getField0)
                        .coGroup(dataQuanta2.keyBy(Tuple2::getField1))
                        .map(joinTuple -> new Tuple2<>(
                                WayangCollections.asSet(joinTuple.getField0()),
                                WayangCollections.asSet(joinTuple.getField1())
                        ))
                        .collect();

        // Verify the outcome.
        Set<Tuple2<Set<Tuple2<String, Integer>>, Set<Tuple2<String, String>>>> expectedValues = WayangCollections.asSet(
                new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Water", 0)),
                        WayangCollections.asSet(new Tuple2<>("Tap water", "Water"))
                ),
                new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Cola", 5)),
                        WayangCollections.asSet()
                ), new Tuple2<>(
                        WayangCollections.asSet(new Tuple2<>("Juice", 10)),
                        WayangCollections.asSet(new Tuple2<>("Apple juice", "Juice"), new Tuple2<>("Orange juice", "Juice"))
                )
        );
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testIntersect() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues1 = Arrays.asList(1, 2, 3, 4, 5, 7, 8, 9, 10);
        final List<Integer> inputValues2 = Arrays.asList(0, 2, 3, 3, 4, 5, 7, 8, 9, 11);

        // Execute the job.
        final LoadCollectionDataQuantaBuilder<Integer> dataQuanta1 = builder.loadCollection(inputValues1);
        final LoadCollectionDataQuantaBuilder<Integer> dataQuanta2 = builder.loadCollection(inputValues2);
        final Collection<Integer> outputValues = dataQuanta1.intersect(dataQuanta2).collect();

        // Verify the outcome.
        Set<Integer> expectedValues = WayangCollections.asSet(2, 3, 4, 5, 7, 8, 9);
        Assert.assertEquals(expectedValues, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testSort() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        final List<Integer> inputValues1 = Arrays.asList(3, 4, 5, 2, 1);

        // Execute the job.
        final LoadCollectionDataQuantaBuilder<Integer> dataQuanta1 = builder.loadCollection(inputValues1);
        final Collection<Integer> outputValues = dataQuanta1.sort(r -> r).collect();

        // Verify the outcome.
        List<Integer> expectedValues = Arrays.asList(1, 2, 3, 4, 5);
        Assert.assertEquals(expectedValues, WayangCollections.asList(outputValues));
    }


    @Test
    public void testPageRank() {
        // Set up WayangContext.
        WayangContext wayangContext = new WayangContext()
                .with(Java.basicPlugin())
                .with(Java.graphPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

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
                .collect();
        List<Tuple2<Long, Float>> sortedPageRanks = new ArrayList<>(pageRanks);
        sortedPageRanks.sort((pr1, pr2) -> Float.compare(pr2.field1, pr1.field1));

        System.out.println(sortedPageRanks);
        Assert.assertEquals(1L, sortedPageRanks.get(0).field0.longValue());
        Assert.assertEquals(0L, sortedPageRanks.get(1).field0.longValue());
        Assert.assertEquals(2L, sortedPageRanks.get(2).field0.longValue());
        Assert.assertEquals(3L, sortedPageRanks.get(3).field0.longValue());
    }

    @Test
    public void testMapPartitions() {
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

        // Generate test data.
        List<Integer> inputValues = WayangArrays.asList(0, 1, 2, 3, 4, 6, 8);

        // Execute the job.
        Collection<Tuple2<String, Integer>> outputValues = builder.loadCollection(inputValues)
                .mapPartitions(partition -> {
                    int numEvens = 0, numOdds = 0;
                    for (Integer value : partition) {
                        if ((value & 1) == 0) numEvens++;
                        else numOdds++;
                    }
                    return Arrays.asList(
                            new Tuple2<>("odd", numOdds),
                            new Tuple2<>("even", numEvens)
                    );
                })
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .collect();

        // Check the output.
        Set<Tuple2<String, Integer>> expectedOutput = WayangCollections.asSet(
                new Tuple2<>("even", 5), new Tuple2<>("odd", 2)
        );
        Assert.assertEquals(expectedOutput, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testZipWithId() {
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

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
                .groupByKey(Tuple2::getField1)
                .map(group -> {
                    int distinctIds = (int) StreamSupport.stream(group.spliterator(), false)
                            .map(Tuple2::getField0)
                            .distinct()
                            .count();
                    return new Tuple2<>(distinctIds, 1);
                })
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .collect();

        // Check the output.
        Set<Tuple2<Integer, Integer>> expectedOutput = Collections.singleton(new Tuple2<>(42, 100));
        Assert.assertEquals(expectedOutput, WayangCollections.asSet(outputValues));
    }

    @Test
    public void testWriteTextFile() throws IOException, URISyntaxException {
        WayangContext wayangContext = new WayangContext().with(Java.basicPlugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangContext);

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
        final WayangContext wayangCtx = new WayangContext(this.sqlite3Configuration)
                .with(Java.basicPlugin())
                .with(Sqlite3.plugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangCtx, "testSqlOnJava()");
        final Collection<String> outputValues = builder
                .readTable(new Sqlite3TableSource("customer", "name", "age"))
                .filter(r -> (Integer) r.getField(1) >= 18).withSqlUdf("age >= 18").withTargetPlatform(Java.platform())
                .asRecords().projectRecords(new String[]{"name"})
                .map(record -> (String) record.getField(0))
                .collect();

        // Test the outcome.
        Assert.assertEquals(
                WayangCollections.asSet("John", "Evelyn"),
                WayangCollections.asSet(outputValues)
        );
    }

    @Test
    public void testSqlOnSqlite3() throws IOException, SQLException {
        // Execute job.
        final WayangContext wayangCtx = new WayangContext(this.sqlite3Configuration)
                .with(Java.basicPlugin())
                .with(Sqlite3.plugin());
        JavaPlanBuilder builder = new JavaPlanBuilder(wayangCtx, "testSqlOnSqlite3()");
        final Collection<String> outputValues = builder
                .readTable(new Sqlite3TableSource("customer", "name", "age"))
                .filter(r -> (Integer) r.getField(1) >= 18).withSqlUdf("age >= 18")
                .asRecords().projectRecords(new String[]{"name"}).withTargetPlatform(Sqlite3.platform())
                .map(record -> (String) record.getField(0))
                .collect();

        // Test the outcome.
        Assert.assertEquals(
                WayangCollections.asSet("John", "Evelyn"),
                WayangCollections.asSet(outputValues)
        );
    }

}
