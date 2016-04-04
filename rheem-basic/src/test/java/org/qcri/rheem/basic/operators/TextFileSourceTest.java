package org.qcri.rheem.basic.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Optional;

/**
 * Test suite for {@link TextFileSource}.
 */
public class TextFileSourceTest {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void testCardinalityEstimation() throws URISyntaxException, IOException {
        final URL testFile = this.getClass().getResource("/ulysses.txt");
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

        Configuration configuration = new Configuration();
        final Optional<CardinalityEstimator> cardinalityEstimator = textFileSource
                .getCardinalityEstimator(0, configuration);

        Assert.assertTrue(cardinalityEstimator.isPresent());
        final CardinalityEstimate estimate = cardinalityEstimator.get().estimate(configuration);

        this.logger.info("Estimated between {} and {} lines in {} and counted {}.",
                estimate.getLowerEstimate(),
                estimate.getUpperEstimate(),
                testFile,
                numLineFeeds);

        Assert.assertTrue(estimate.getLowerEstimate() <= numLineFeeds);
        Assert.assertTrue(estimate.getUpperEstimate() >= numLineFeeds);
    }

}
