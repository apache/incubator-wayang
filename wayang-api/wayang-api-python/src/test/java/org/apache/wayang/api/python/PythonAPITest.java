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

package org.apache.wayang.api.python;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.Assert;
import org.junit.Test;

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
import java.io.Serializable;
import java.net.URL;
import java.util.Arrays;

import com.google.protobuf.ByteString;

public class PythonAPITest {

    @Test
    public void testSomething() {
        Assert.assertEquals(true, true);
    }

    public static URL createUri(String resourcePath) {
        try {
            return Thread.currentThread().getContextClassLoader().getResource(resourcePath);
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    @Test
    public void testOnJava() throws IOException {

        URL FILE_SOME_LINES_TXT = createUri("python-lines.txt");

        // Instantiate Wayang and activate the backend.
        WayangContext context = new WayangContext().with(Java.basicPlugin());
        TextFileSource textFileSource = new TextFileSource(FILE_SOME_LINES_TXT.toString());

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator
                = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        line -> Arrays.asList(
                                (String[]) line.split(" ")
                        ),
                        String.class,
                        String.class
                )
        );

        SortOperator<String, String> sortJava
                = new SortOperator<String, String>(
                new TransformationDescriptor<String, String>(
                        l -> l.toLowerCase(),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasic(String.class)
                )
        );

        String pythonUDF = "lambda x: (str(y) + \" Test\" for y in x)";

        MapPartitionsOperator<String, String> sortPython =
                new MapPartitionsOperator<String, String>(
                        new MapPartitionsDescriptor<String, String>(
                                new WrappedPythonFunction<String, String>(
                                        l -> l,
                                        ByteString.copyFromUtf8(pythonUDF)
                                ),
                                String.class,
                                String.class
                        )
                );

        LocalCallbackSink<String> sink = LocalCallbackSink.createStdoutSink(String.class);

        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, sortPython, 0);
        sortPython.connectTo(0, sink, 0);

        context.execute(new WayangPlan(sink));
    }

}
