package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;
import org.qcri.rheem.java.plugin.JavaPlatform;
import org.qcri.rheem.tests.platform.MyMadeUpPlatform;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test the Java integration with Rheem.
 */
public class JavaIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        List<String> collector = new LinkedList<>();
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefault(String.class));
        textFileSource.connectTo(0, sink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(sink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);

        // Verify the plan result.
        final List<String> lines = Files.lines(Paths.get(inputUrl.toURI())).collect(Collectors.toList());
        Assert.assertEquals(lines, collector);
    }

    @Test
    public void testReadAndTransformAndWrite() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration1() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        // ILLEGAL: This platform is not registered, so this operator will find no implementation.
        textFileSource.addTargetPlatform(MyMadeUpPlatform.getInstance());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration2() throws URISyntaxException {
        RheemContext rheemContext = new RheemContext();
        // ILLEGAL: This dummy platform is not sufficient to execute the plan.
        rheemContext.register(MyMadeUpPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test(expected = RheemException.class)
    public void testReadAndTransformAndWriteWithIllegalConfiguration3() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        // ILLEGAL:
        textFileSource.addTargetPlatform(MyMadeUpPlatform.getInstance());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        final Job job = rheemContext.createJob(rheemPlan);
        // ILLEGAL: We blacklist the Java platform, although we need it.
        job.getConfiguration().getPlatformProvider().addToBlacklist(JavaPlatform.getInstance());
        job.getConfiguration().getPlatformProvider().addToWhitelist(MyMadeUpPlatform.getInstance());
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testMultiSourceAndMultiSink() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Define some input data.
        final List<String> collection1 = Arrays.<String>asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.<String>asList("This is source 2.", "This is source 2, too.");

        // Build a Rheem plan.
        RheemPlan rheemPlan = new RheemPlan();

        final DataSetType<String> stringDataSet = DataSetType.createDefault(String.class);
        CollectionSource<String> source1 = new CollectionSource<>(
                collection1,
                stringDataSet);

        CollectionSource<String> source2 = new CollectionSource<>(
                collection2,
                stringDataSet);

        CoalesceOperator<String> coalesceOperator = new CoalesceOperator<>(stringDataSet);
        source1.connectTo(0, coalesceOperator, 0);
        source2.connectTo(0, coalesceOperator, 1);

        MapOperator<String, String> uppercaseOperator = new MapOperator<>(
                stringDataSet,
                stringDataSet,
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        coalesceOperator.connectTo(0, uppercaseOperator, 0);

        List<String> collector1 = new LinkedList<>();
        LocalCallbackSink<String> sink1 = LocalCallbackSink.createCollectingSink(collector1, stringDataSet);
        uppercaseOperator.connectTo(0, sink1, 0);
        rheemPlan.addSink(sink1);

        List<String> collector2 = new LinkedList<>();
        LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(collector2, stringDataSet);
        coalesceOperator.connectTo(0, sink2, 0);
        rheemPlan.addSink(sink2);

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
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Define some input data.
        final List<String> collection1 = Arrays.<String>asList("This is source 1.", "This is source 1, too.");
        final List<String> collection2 = Arrays.<String>asList("This is source 2.", "This is source 2, too.");

        // Build a Rheem plan.
        RheemPlan rheemPlan = new RheemPlan();

        final DataSetType<String> stringDataSet = DataSetType.createDefault(String.class);
        CollectionSource<String> source1 = new CollectionSource<>(
                collection1,
                stringDataSet);

        CollectionSource<String> source2 = new CollectionSource<>(
                collection2,
                stringDataSet);

        CoalesceOperator<String> coalesceOperator1 = new CoalesceOperator<>(stringDataSet);
        source1.connectTo(0, coalesceOperator1, 0);
        source2.connectTo(0, coalesceOperator1, 1);

        MapOperator<String, String> lowerCaseOperator = new MapOperator<>(
                stringDataSet,
                stringDataSet,
                new TransformationDescriptor<>(
                        String::toLowerCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        coalesceOperator1.connectTo(0, lowerCaseOperator, 0);

        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                stringDataSet,
                stringDataSet,
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        coalesceOperator1.connectTo(0, upperCaseOperator, 0);

        CoalesceOperator<String> coalesceOperator2 = new CoalesceOperator<>(stringDataSet);
        lowerCaseOperator.connectTo(0, coalesceOperator2, 0);
        upperCaseOperator.connectTo(0, coalesceOperator2, 1);


        List<String> collector1 = new LinkedList<>();
        LocalCallbackSink<String> sink1 = LocalCallbackSink.createCollectingSink(collector1, stringDataSet);
        coalesceOperator2.connectTo(0, sink1, 0);
        rheemPlan.addSink(sink1);

        List<String> collector2 = new LinkedList<>();
        LocalCallbackSink<String> sink2 = LocalCallbackSink.createCollectingSink(collector2, stringDataSet);
        coalesceOperator2.connectTo(0, sink2, 0);
        rheemPlan.addSink(sink2);

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
    public void testFullScenario1() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        StdoutSink<Long> stdoutSink = new StdoutSink<>(DataSetType.createDefault(Long.class));
        CountOperator<String> countLinesOperator = new CountOperator<>(DataSetType.createDefault(String.class));
        DistinctOperator<String> distinctLinesOperator = new DistinctOperator<>(DataSetType.createDefault(String.class));
        SortOperator<String> sortOperator = new SortOperator<>(DataSetType.createDefault(String.class));

        textFileSource.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, countLinesOperator, 0);
        countLinesOperator.connectTo(0, stdoutSink, 0); // 5 distinct lines, 6 total


        // Execute Rheem plan
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testFullScenario2() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        rheemContext.register(JavaPlatform.getInstance());

        // Build a Rheem plan.
        final URL inputUrl = this.getClass().getResource("/some-lines.txt");
        final URL inputUrl1 = this.getClass().getResource("/other-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        TextFileSource textFileSource1 = new TextFileSource(inputUrl1.toURI().toString());
        FilterOperator<String> noCommaOperator = new FilterOperator<>(
                DataSetType.createDefault(String.class),
                new Predicate<String>() {
                    @Override
                    public boolean test(String s) {
                        return !s.contains(",");
                    }
                });
        MapOperator<String, String> upperCaseOperator = new MapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class),
                new TransformationDescriptor<>(
                        String::toUpperCase,
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)));
        UnionAllOperator<String> unionOperator = new UnionAllOperator<>(DataSetType.createDefault(String.class));
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        DistinctOperator<String> distinctLinesOperator = new DistinctOperator<>(DataSetType.createDefault(String.class));
        SortOperator<String> sortOperator = new SortOperator<>(DataSetType.createDefault(String.class));

        // Read from file 1, remove commas, union with file 2, sort, upper case, then remove duplicates and output.
        textFileSource.connectTo(0, noCommaOperator, 0);
        textFileSource1.connectTo(0, unionOperator, 0);
        noCommaOperator.connectTo(0, unionOperator, 1);
        unionOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, stdoutSink, 0);


        // Execute Rheem plan
        RheemPlan rheemPlan = new RheemPlan();
        rheemPlan.addSink(stdoutSink);
        rheemContext.execute(rheemPlan);
    }
}
