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

package org.apache.wayang.java.operators;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.fs.LocalFileSystem;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link JavaTextFileSource}.
 */
public class JavaTextFileSourceTest extends JavaExecutionOperatorTestBase {

    private Locale defaultLocale;

    /**
     * In locales, where the decimal separator is not "." this rest would fail.
     * Therefore we ensure it's run in a pre-defined locale and we make sure it's
     * reset after the test.
     */
    @Before
    public void setupTest() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @After
    public void teardownTest() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    public void testReadLocalFile() throws IOException, URISyntaxException {
        final String testFileName = "/banking-tx-small.csv";

        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = this.getClass().getResource(testFileName);
            System.out.println( "* " + inputUrl + " *");
            JavaTextFileSource source = new JavaTextFileSource(
                    inputUrl.toString() );

            // Execute.
            JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
            JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            // Verify the outcome.
            final List<String> result = outputs[0].<String>provideStream().collect(Collectors.toList());
            Assert.assertEquals(63, result.size());
        } finally {
            if (javaExecutor != null) javaExecutor.dispose();
        }
    }

    // @Test
    /**
     * Requires a local HTTP Server running, in the project root ...
     *
     * With Python 3: python -m http.server
     * With Python 2: python -m SimpleHTTPServer
     */
    public void testReadRemoteFileHTTP() throws IOException, URISyntaxException {
        final String testFileURL = "http://localhost:8000/LICENSE";

        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = new URL(testFileURL);
            System.out.println( "** " + inputUrl + " **");
            JavaTextFileSource source = new JavaTextFileSource(
                    inputUrl.toString() );

            // Execute.
            JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
            JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            // Verify the outcome.
            final List<String> result = outputs[0].<String>provideStream().collect(Collectors.toList());
            Assert.assertEquals(225, result.size());
        } finally {
            if (javaExecutor != null) javaExecutor.dispose();
        }
    }

    @Test
    public void testReadRemoteFileHTTPS() throws IOException, URISyntaxException {
        final String testFileURL = "https://kamir.solidcommunity.net/public/ecolytiq-sustainability-profile/profile2.ttl";

        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = new URL(testFileURL);
            System.out.println( "*** " + inputUrl + " ***");
            JavaTextFileSource source = new JavaTextFileSource(
                    inputUrl.toString() );

            // Execute.
            JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
            JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            // Verify the outcome.
            final List<String> result = outputs[0].<String>provideStream().collect(Collectors.toList());
            Assert.assertEquals(23, result.size());
        } finally {
            if (javaExecutor != null) javaExecutor.dispose();
        }

    }
}
