package org.qcri.rheem.tests;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.basic.operators.LocalCallbackSink;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.types.BasicDataUnitType;
import org.qcri.rheem.core.types.DataSet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Test the Java integration with Rheem.
 */
public class JavaIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException, IOException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.registerTo(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        List<String> collector = new LinkedList<>();
        LocalCallbackSink<String> sink = LocalCallbackSink.createCollectingSink(collector, DataSet.flatAndBasic(String.class));
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
        org.qcri.rheem.java.plugin.Activator.registerTo(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSet.flatAndBasic(String.class),
                DataSet.flatAndBasic(String.class),
                new TransformationDescriptor<>(String::toUpperCase,
                        new BasicDataUnitType(String.class),
                        new BasicDataUnitType(String.class)));
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSet.flatAndBasic(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

}
