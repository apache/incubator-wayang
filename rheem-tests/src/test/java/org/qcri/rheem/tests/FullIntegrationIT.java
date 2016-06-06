package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.operators.CollectionSource;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.MaterializedGroupByOperator;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.qcri.rheem.tests.platform.MyMadeUpPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Test the Java integration with Rheem.
 */
public class FullIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        List<String> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

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
        sink.addTargetPlatform(SparkPlatform.getInstance());
        final Operator source = sink.getInputOperator(0);
        source.addTargetPlatform(JavaPlatform.getInstance());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

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
    public void testDiverseScenario1() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario1(RheemPlans.FILE_SOME_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario2() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario3() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario4() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario4(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testSimpleSingleStageLoop() throws URISyntaxException {
        // Build the RheemPlan.
        final Set<Integer> collector = new HashSet<>();
        RheemPlan rheemPlan = RheemPlans.simpleLoop(3, collector, 0, 1, 2);

        rheemPlan.collectTopLevelOperatorByName("source").addTargetPlatform(SparkPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("convergenceSource").addTargetPlatform(SparkPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("loop").addTargetPlatform(JavaPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("step").addTargetPlatform(JavaPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("counter").addTargetPlatform(JavaPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("sink").addTargetPlatform(SparkPlatform.getInstance());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        rheemContext.execute(rheemPlan);

        final HashSet<Integer> expected = new HashSet<>(RheemArrays.asList(RheemArrays.range(0, 24)));
        Assert.assertEquals(expected, collector);
    }

    @Test
    public void testSimpleMultiStageLoop() throws URISyntaxException {
        // Build the RheemPlan.
        final List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.simpleLoop(3, collector, 0, 1, 2);

        rheemPlan.collectTopLevelOperatorByName("source").addTargetPlatform(SparkPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("convergenceSource").addTargetPlatform(SparkPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("loop").addTargetPlatform(JavaPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("step").addTargetPlatform(SparkPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("counter").addTargetPlatform(JavaPlatform.getInstance());
        rheemPlan.collectTopLevelOperatorByName("sink").addTargetPlatform(SparkPlatform.getInstance());

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        rheemContext.execute(rheemPlan);
        final HashSet<Integer> expected = new HashSet<>(RheemArrays.asList(RheemArrays.range(0, 24)));
        Assert.assertEquals(expected, RheemCollections.asSet(collector));
    }

    @Test
    public void testSimpleSample() throws URISyntaxException {
        // Build the RheemPlan.
        final List<Integer> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.simpleSample(collector, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        rheemContext.execute(rheemPlan);
        System.out.println(collector);
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

        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());
        rheemContext.register(JavaPlatform.getInstance());

        rheemContext.execute(new RheemPlan(sink));
        System.out.println(collector);
    }
}
