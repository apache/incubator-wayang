package org.qcri.rheem.core.optimizer.cardinality;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for {@link AggregatingCardinalityEstimator}.
 */
public class AggregatingCardinalityEstimatorTest {

    @Test
    public void testEstimate() {
        OptimizationContext optimizationContext = mock(OptimizationContext.class);
        when(optimizationContext.getConfiguration()).thenReturn(new Configuration());

        CardinalityEstimator partialEstimator1 = new DefaultCardinalityEstimator(0.9, 1, false, cards -> cards[0] * 2);
        CardinalityEstimator partialEstimator2 = new DefaultCardinalityEstimator(0.8, 1, false, cards -> cards[0] * 3);
        CardinalityEstimator estimator = new AggregatingCardinalityEstimator(
                Arrays.asList(partialEstimator1, partialEstimator2)
        );

        CardinalityEstimate inputEstimate = new CardinalityEstimate(10, 100, 0.3);
        CardinalityEstimate outputEstimate = estimator.estimate(optimizationContext, inputEstimate);
        CardinalityEstimate expectedEstimate = new CardinalityEstimate(2 * 10, 2 * 100, 0.3 * 0.9);

        Assert.assertEquals(expectedEstimate, outputEstimate);
    }


}
