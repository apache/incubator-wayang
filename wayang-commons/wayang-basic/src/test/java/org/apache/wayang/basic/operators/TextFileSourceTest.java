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

package org.apache.wayang.basic.operators;


import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.junit.Assert;
import org.junit.Test;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link TextFileSource}.
 */
public class TextFileSourceTest {

    private final Logger logger = LogManager.getLogger(this.getClass());

    @Test
    public void testCardinalityEstimation() throws URISyntaxException, IOException {
        Job job = mock(Job.class);
        DefaultOptimizationContext optimizationContext = mock(DefaultOptimizationContext.class);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        when(optimizationContext.getJob()).thenReturn(job);
        when(job.getStopWatch()).thenReturn(new StopWatch(new Experiment("mock", new Subject("mock", "mock"))));
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());
        final URL testFile = this.getClass().getResource("/text.input");
        final TextFileSource textFileSource = new TextFileSource(testFile.toString());

        final BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(new File(testFile.toURI())),
                        textFileSource.getEncoding()
                )
        );

        // Read as much as possible.
        char[] cbuf = new char[1024];
        int numReadChars, numLineFeeds = 0;
        while ((numReadChars = bufferedReader.read(cbuf)) != -1) {
            for (int i = 0; i < numReadChars; i++) {
                if (cbuf[i] == '\n') {
                    numLineFeeds++;
                }
            }
        }

        final Optional<CardinalityEstimator> cardinalityEstimator = textFileSource
                .createCardinalityEstimator(0, optimizationContext.getConfiguration());

        Assert.assertTrue(cardinalityEstimator.isPresent());
        final CardinalityEstimate estimate = cardinalityEstimator.get().estimate(optimizationContext);

        this.logger.info("Estimated between {} and {} lines in {} and counted {}.",
                estimate.getLowerEstimate(),
                estimate.getUpperEstimate(),
                testFile,
                numLineFeeds);

        Assert.assertTrue(estimate.getLowerEstimate() <= numLineFeeds);
        Assert.assertTrue(estimate.getUpperEstimate() >= numLineFeeds);
    }

}
