package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.costs.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;
import org.qcri.rheem.core.plan.UnarySource;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.LimitedInputStream;
import org.qcri.rheem.core.util.fs.FileSystem;
import org.qcri.rheem.core.util.fs.FileSystems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * This source reads a text file and outputs the lines as data units.
 */
public class TextFileSource extends UnarySource {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final String inputUrl;

    private final String encoding;

    public TextFileSource(String inputUrl) {
        this(inputUrl, "UTF-8");
    }

    public TextFileSource(String inputUrl, String encoding) {
        super(DataSetType.createDefault(String.class), null);
        this.inputUrl = inputUrl;
        this.encoding = encoding;
    }

    public String getInputUrl() {
        return inputUrl;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.costs.CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Map<OutputSlot<?>, CardinalityEstimate> cache) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new TextFileSource.CardinalityEstimator(this.getOutput(), cache));
    }

    public String getEncoding() {
        return encoding;
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.costs.CardinalityEstimator} for {@link FlatMapOperator}s.
     */
    private class CardinalityEstimator extends org.qcri.rheem.core.optimizer.costs.CardinalityEstimator.WithCache {

        public final CardinalityEstimate FALLBACK_ESTIMATE = new CardinalityEstimate(1000L, 100000000L, 0.7);

        public static final double CORRECTNESS_PROBABILITY = 0.95d;

        /**
         * We expect selectivities to be correct within a factor of {@value #EXPECTED_ESTIMATE_DEVIATION}.
         */
        public static final double EXPECTED_ESTIMATE_DEVIATION = 0.05;

        public CardinalityEstimator(OutputSlot<?> targetOutput, Map<OutputSlot<?>, CardinalityEstimate> estimateCache) {
            super(targetOutput, estimateCache);
        }

        @Override
        public CardinalityEstimate calculateEstimate(RheemContext rheemContext, CardinalityEstimate... inputEstimates) {
            Validate.inclusiveBetween(0, TextFileSource.this.getNumOutputs() - 1, inputEstimates.length);

            OptionalLong fileSize = determineFileSize(TextFileSource.this.inputUrl);
            if (!fileSize.isPresent()) {
                TextFileSource.this.logger.warn("Could not determine size of {}... deliver fallback estimate.",
                        TextFileSource.this.inputUrl);
                return FALLBACK_ESTIMATE;
            } else if (fileSize.getAsLong() == 0L) {
                return new CardinalityEstimate(0L, 0L, 1d);
            }

            OptionalDouble bytesPerLine = estimateBytesPerLine();
            if (!bytesPerLine.isPresent()) {
                TextFileSource.this.logger.warn("Could not determine average line size of {}... deliver fallback estimate.",
                        TextFileSource.this.inputUrl);
                return FALLBACK_ESTIMATE;
            }

            double numEstimatedLines = fileSize.getAsLong() / bytesPerLine.getAsDouble();
            double expectedDeviation = numEstimatedLines * EXPECTED_ESTIMATE_DEVIATION;

            return new CardinalityEstimate(
                    (long) (numEstimatedLines - expectedDeviation),
                    (long) (numEstimatedLines + expectedDeviation),
                    CORRECTNESS_PROBABILITY
            );
        }

        /**
         * Determine the number of bytes of a given file.
         *
         * @param inputUrl the URL of the file
         * @return the number of bytes of the file if it could be determined
         */
        private OptionalLong determineFileSize(String inputUrl) {
            final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(inputUrl);
            if (fileSystem.isPresent()) {
                try {
                    return OptionalLong.of(fileSystem.get().getFileSize(inputUrl));
                } catch (FileNotFoundException e) {
                    logger.warn("Could not determine file size.", e);
                }
            }

            return OptionalLong.empty();
        }

        /**
         * Estimate the number of bytes that are in each line of a given file.
         *
         * @return the average number of bytes per line if it could be determined
         */
        private OptionalDouble estimateBytesPerLine() {
            final Optional<FileSystem> fileSystem = FileSystems.getFileSystem(inputUrl);
            if (fileSystem.isPresent()) {

                // Construct a limited reader for the first x KiB of the file.
                final int KiB = 1024;
                final int MiB = 1024 * KiB;
                try (LimitedInputStream lis = new LimitedInputStream(fileSystem.get().open(inputUrl), 1 * MiB)) {
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
                        logger.warn("Could not find any newline character in {}.", inputUrl);
                        return OptionalDouble.empty();
                    }
                    return OptionalDouble.of((double) lis.getNumReadBytes() / numLineFeeds);
                } catch (IOException e) {
                    logger.error("Could not estimate bytes per line of an input file.", e);
                }
            }

            return OptionalDouble.empty();
        }
    }

}
