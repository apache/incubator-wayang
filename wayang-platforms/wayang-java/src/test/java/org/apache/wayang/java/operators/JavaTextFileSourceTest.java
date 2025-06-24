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

import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.execution.JavaExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test suite for {@link JavaTextFileSource}.
 */
class JavaTextFileSourceTest extends JavaExecutionOperatorTestBase {

    private Locale defaultLocale;

    /**
     * In locales, where the decimal separator is not "." this rest would fail.
     * Therefore we ensure it's run in a pre-defined locale and we make sure it's
     * reset after the test.
     */
    @BeforeEach
    void setupTest() {
        defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.US);
    }

    @AfterEach
    void teardownTest() {
        Locale.setDefault(defaultLocale);
    }

    @Test
    void testReadLocalFile() {
        final String testFileName = "/banking-tx-small.csv";

        // Prepare the source.
        final URL inputUrl = this.getClass().getResource(testFileName);
        System.out.println("* " + inputUrl + " *");
        final JavaTextFileSource source = new JavaTextFileSource(
                inputUrl.toString());

        // Execute.
        final JavaChannelInstance[] inputs = new JavaChannelInstance[] {};
        final JavaChannelInstance[] outputs = new JavaChannelInstance[] { createStreamChannelInstance() };
        evaluate(source, inputs, outputs);

        // Verify the outcome.
        final List<String> result = outputs[0].<String>provideStream().toList();
        assertEquals(63, result.size());
    }

    /**
     * Requires a local HTTP Server running, in the project root ...
     *
     * With Python 3: python -m http.server
     * With Python 2: python -m SimpleHTTPServer
     */
    @Disabled 
    @Test
    void testReadRemoteFileHTTP() throws Exception {
        final String testFileURL = "http://localhost:8000/LICENSE";

        final JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = new URL(testFileURL);
            System.out.println("** " + inputUrl + " **");
            final JavaTextFileSource source = new JavaTextFileSource(
                    inputUrl.toString());

            // Execute.
            final JavaChannelInstance[] inputs = new JavaChannelInstance[] {};
            final JavaChannelInstance[] outputs = new JavaChannelInstance[] { createStreamChannelInstance() };
            evaluate(source, inputs, outputs);

            // Verify the outcome.
            final List<String> result = outputs[0].<String>provideStream().toList();
            assertEquals(225, result.size());
        } finally {
            if (javaExecutor != null)
                javaExecutor.dispose();
        }
    }

    @Test
    void testReadRemoteFileHTTPS() throws Exception {
        final String testFileURL = "https://downloads.apache.org/incubator/wayang/1.0.0/RELEASE_NOTES";

        // Prepare the source.
        final URL inputUrl = new URL(testFileURL);
        final JavaTextFileSource source = new JavaTextFileSource(inputUrl.toString());

        // Execute.
        final JavaChannelInstance[] inputs = new JavaChannelInstance[] {};
        final JavaChannelInstance[] outputs = new JavaChannelInstance[] { createStreamChannelInstance() };
        evaluate(source, inputs, outputs);

        // Verify the outcome.
        final List<String> result = outputs[0].<String>provideStream().toList();
        assertEquals(64, result.size());
    }
}
