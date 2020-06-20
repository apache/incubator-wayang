package org.qcri.rheem.basic.operators;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Test
    public void testCardinalityEstimation() throws URISyntaxException, IOException {
        Job job = mock(Job.class);
        DefaultOptimizationContext optimizationContext = mock(DefaultOptimizationContext.class);
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
        when(optimizationContext.getJob()).thenReturn(job);
        when(job.getStopWatch()).thenReturn(new StopWatch(new Experiment("mock", new Subject("mock", "mock"))));
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());
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
