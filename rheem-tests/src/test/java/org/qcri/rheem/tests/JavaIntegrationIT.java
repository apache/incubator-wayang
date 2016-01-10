package org.qcri.rheem.tests;

import org.junit.Test;
import org.qcri.rheem.basic.function.StringReverseDescriptor;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.StdoutSink;
import org.qcri.rheem.basic.operators.TextFileSource;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.PhysicalPlan;
import org.qcri.rheem.core.types.DataSet;

import java.net.URISyntaxException;
import java.net.URL;

/**
 * Test the Java integration with Rheem.
 */
public class JavaIntegrationIT {

    @Test
    public void testReadAndWrite() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.registerTo(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSet.flatAndBasic(String.class));
        textFileSource.connectTo(0, stdoutSink, 0);
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

    @Test
    public void testReadAndReverseAndWrite() throws URISyntaxException {
        // Instantiate Rheem and activate the Java backend.
        RheemContext rheemContext = new RheemContext();
        org.qcri.rheem.java.plugin.Activator.registerTo(rheemContext);

        // Build a Rheem plan.
        final URL inputUrl = getClass().getResource("/some-lines.txt");
        TextFileSource textFileSource = new TextFileSource(inputUrl.toURI().toString());
        MapOperator<String, String> reverseOperator = new MapOperator<>(
                DataSet.flatAndBasic(String.class),
                DataSet.flatAndBasic(String.class),
                new StringReverseDescriptor());
        textFileSource.connectTo(0, reverseOperator, 0);
        StdoutSink<String> stdoutSink = new StdoutSink<>(DataSet.flatAndBasic(String.class));
        reverseOperator.connectTo(0, stdoutSink, 0);
        PhysicalPlan rheemPlan = new PhysicalPlan();
        rheemPlan.addSink(stdoutSink);

        // Have Rheem execute the plan.
        rheemContext.execute(rheemPlan);
    }

}
