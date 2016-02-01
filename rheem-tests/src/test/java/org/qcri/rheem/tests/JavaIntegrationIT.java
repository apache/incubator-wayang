package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.types.DataUnitType;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Test the Java integration with Rheem.
 */
public class JavaIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.activate(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        List<String> collector = new LinkedList<>();
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, DataSetType.createDefault(String.class));
        textFileSource.connectTo(0, sink, 0);
        PhysicalPlan rheemPlan = new PhysicalPlan();
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
        org.qcri.rheem.java.plugin.Activator.activate(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
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
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testFullScenario1() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.activate(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
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


        // Execute physical plan
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testFullScenario2() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.activate(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
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
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSetType.createDefault(String.class));
        DistinctOperator<String> distinctLinesOperator = new DistinctOperator<>(DataSetType.createDefault(String.class));
        SortOperator<String> sortOperator = new SortOperator<>(DataSetType.createDefault(String.class));

        textFileSource.connectTo(0, noCommaOperator, 0);
        noCommaOperator.connectTo(0, sortOperator, 0);
        sortOperator.connectTo(0, upperCaseOperator, 0);
        upperCaseOperator.connectTo(0, distinctLinesOperator, 0);
        distinctLinesOperator.connectTo(0, stdoutSink, 0);


        // Execute physical plan
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);
        rheemContext.execute(rheemPlan);
    }
}
