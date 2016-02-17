package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
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

/**
 * Test the Spark integration with Rheem.
 */
public class SparkIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Build a Rheem plan.
        List<String> collector = new LinkedList<>();
        RheemPlan rheemPlan = RheemPlans.readWrite(RheemPlans.FILE_SOME_LINES_TXT, collector);

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
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

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration1() throws URISyntaxException {
        // Build a Rheem plan.
        final RheemPlan rheemPlan = RheemPlans.readTransformWrite(RheemPlans.FILE_SOME_LINES_TXT);
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        rheemPlan.getSinks().forEach(sink -> sink.addTargetPlatform(MyMadeUpPlatform.getInstance()));

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

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
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        // Have Rheem execute the plan.
        final Job job = rheemContext.createJob(rheemPlan);
        // ILLEGAL: We blacklist the Spark platform, although we need it.
        job.getConfiguration().getPlatformProvider().addToBlacklist(SparkPlatform.getInstance());
        job.getConfiguration().getPlatformProvider().addToWhitelist(MyMadeUpPlatform.getInstance());
        job.execute();
    }

    @Test
    public void testMultiSourceAndMultiSink() throws URISyntaxException {
        // Define some input data.
        final List<String> collection1 = Arrays.<String>asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.<String>asList("This is source 2.", "This is source 2, too.");
        List<String> collector1 = new LinkedList<>();
        List<String> collector2 = new LinkedList<>();
        final RheemPlan rheemPlan = RheemPlans.multiSourceMultiSink(collection1, collection2, collector1, collector2);

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
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

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
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

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testDiverseScenario2() throws URISyntaxException {
        // Build the RheemPlan.
        RheemPlan rheemPlan = RheemPlans.diverseScenario2(RheemPlans.FILE_SOME_LINES_TXT, RheemPlans.FILE_OTHER_LINES_TXT);

        // Instantiate Rheem and activate the Spark backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);
    }

    @Ignore
    @Test
    public void testBroadcasts() {
        Collection<Integer> broadcastedValues = Arrays.asList(1, 2, 3, 4);
        Collection<Integer> mainValues = Arrays.asList(2, 4, 6, 2);
        List<Integer> collectedValues = new ArrayList<>();
        List<Integer> expectedValues = Arrays.asList(2, 2, 4);

        final DataSetType<Integer> integerDataSetType = DataSetType.createDefault(Integer.class);
        CollectionSource<Integer> broadcastSource = new CollectionSource<>(broadcastedValues,
                integerDataSetType);
        CollectionSource<Integer> mainSource = new CollectionSource<>(mainValues,
                integerDataSetType);
        FilterOperator<Integer> semijoin = new FilterOperator<>(
                integerDataSetType,
                new PredicateDescriptor.ExtendedSerializablePredicate<Integer>() {

                    private Set<Integer> allowedInts;

                    @Override
                    public void open(ExecutionContext ctx) {
                        this.allowedInts = new HashSet<>(ctx.<Integer>getBroadcast("allowed values"));
                    }

                    @Override
                    public boolean test(Integer integer) {
                        return this.allowedInts.contains(integer);
                    }
                }
        );
        final LocalCallbackSink<Integer> collectingSink = LocalCallbackSink.createCollectingSink(collectedValues,
                integerDataSetType);

        mainSource.connectTo(0, semijoin, 0);
        broadcastSource.broadcastTo(0, semijoin, "allowed values");
        semijoin.connectTo(0, collectingSink, 0);

        RheemPlan rheemPlan = new RheemPlan(collectingSink);

        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(SparkPlatform.getInstance());

        rheemContext.execute(rheemPlan);

        Collections.sort(collectedValues);
        Assert.assertEquals(expectedValues, collectedValues);
    }
}
