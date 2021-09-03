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


import org.apache.commons.lang3.Validate;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.LimitedInputStream;
import org.apache.wayang.core.util.fs.FileSystem;
import org.apache.wayang.core.util.fs.FileSystems;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * This source reads a text file and outputs the lines as data units.
 */
public class TextFileSource extends UnarySource<String> {

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final String inputUrl;

    private final String encoding;

    public TextFileSource(String inputUrl) {
        this(inputUrl, "UTF-8");
    }

    public TextFileSource(String inputUrl, String encoding) {
        super(DataSetType.createDefault(String.class));
        this.inputUrl = inputUrl;
        this.encoding = encoding;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public TextFileSource(TextFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.encoding = that.getEncoding();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    @Override
    public Optional<org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator> createCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new TextFileSource.CardinalityEstimator());
    }

    public String getEncoding() {
        return this.encoding;
    }

    /**
     * Custom {@link org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    protected class CardinalityEstimator implements org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        public static final double CORRECTNESS_PROBABILITY = 0.95d;

        /**
         * We expect selectivities to be correct within a factor of {@value #EXPECTED_ESTIMATE_DEVIATION}.
         */
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        @Override
        public CardinalityEstimate estimate(OptimizationContext optimizationContext, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(TextFileSource.this.getNumInputs() == inputEstimates.length);

            // see Job for StopWatch measurements
            final TimeMeasurement timeMeasurement = optimizationContext.getJob().getStopWatch().start(
                    "Optimization", "Cardinality&Load Estimation", "Push Estimation", "Estimate source cardinalities"
            );

            // Query the job cache first to see if there is already an estimate.
            String jobCacheKey = String.format("%s.estimate(%s)", this.getClass().getCanonicalName(), TextFileSource.this.inputUrl);
            CardinalityEstimate cardinalityEstimate = optimizationContext.queryJobCache(jobCacheKey, CardinalityEstimate.class);
            if (cardinalityEstimate != null) return  cardinalityEstimate;

            // Otherwise calculate the cardinality.
            // First, inspect the size of the file and its line sizes.
            OptionalLong fileSize = FileSystems.getFileSize(TextFileSource.this.inputUrl);
            if (!fileSize.isPresent()) {
                TextFileSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        TextFileSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;

            } else if (fileSize.getAsLong() == 0L) {
                timeMeasurement.stop();
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalDouble bytesPerLine = this.estimateBytesPerLine();
            if (!bytesPerLine.isPresent()) {
                TextFileSource.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                        TextFileSource.this.inputUrl);
                timeMeasurement.stop();
                return this.FALLBACK_ESTIMATE;
            }

            // Extrapolate a cardinality estimate for the complete file.
            double numEstimatedLines = fileSize.getAsLong() / bytesPerLine.getAsDouble();
            double expectedDeviation = numEstimatedLines * EXPECTED_ESTIMATE_DEVIATION;
            cardinalityEstimate = new CardinalityEstimate(
                    (long) (numEstimatedLines - expectedDeviation),
                    (long) (numEstimatedLines + expectedDeviation),
                    CORRECTNESS_PROBABILITY
            );

            // Cache the result, so that it will not be recalculated again.
            optimizationContext.putIntoJobCache(jobCacheKey, cardinalityEstimate);

            timeMeasurement.stop();
            return cardinalityEstimate;
        }

        /**
         * Estimate the number of bytes that are in each line of a given file.
         *
         * @return the average number of bytes per line if it could be determined
         */
        private OptionalDouble estimateBytesPerLine() {
            final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(TextFileSource.this.inputUrl);
            if (fileSystem.isPresent()) {

                // Construct a limited reader for the first x KiB of the file.
                final int KiB = 1024;
                final int MiB = 1024 * KiB;
                try (LimitedInputStream lis = new LimitedInputStream(fileSystem.get().open(TextFileSource.this.inputUrl), 1 * MiB)) {
                    final BufferedReader bufferedReader = new BufferedReader(
                            new InputStreamReader(lis, TextFileSource.this.encoding)
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

                    if (numLineFeeds == 0) {
                        TextFileSource.this.logger.warn("Could not find any newline character in {}.", TextFileSource.this.inputUrl);
                        return OptionalDouble.empty();
                    }
                    return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
                } catch (IOException e) {
                    TextFileSource.this.logger.error("Could not estimate bytes per line of an input file.", e);
                }
            }

            return OptionalDouble.empty();
        }
    }

}
