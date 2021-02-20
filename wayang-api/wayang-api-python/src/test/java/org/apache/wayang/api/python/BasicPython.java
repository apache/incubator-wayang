package org.apache.wayang.api.python;

import org.apache.wayang.api.python.function.WrappedPythonFunction;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.MapPartitionsDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.java.Java;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class BasicPython {

    public static URI createUri(String resourcePath) {
        try {
            return Thread.currentThread().getClass().getResource(resourcePath).toURI();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    @Test
    public void testOnJava() throws URISyntaxException, IOException {

        URI FILE_SOME_LINES_TXT = createUri("/python-lines.txt");

        // Instantiate Rheem and activate the backend.
        WayangContext rheemContext = new WayangContext().with(Java.basicPlugin());
        TextFileSource textFileSource = new TextFileSource(FILE_SOME_LINES_TXT.toString());

        TextFileSink<String> sink = new TextFileSink<String>(
                "file:///Users/rodrigopardomeza/wayang/incubator-wayang/wayang-api/wayang-api-python/src/main/resources/basic_output.txt",
                String.class
        );

        textFileSource.connectTo(0, sink, 0);

        rheemContext.execute(new WayangPlan(sink));
    }
}

