package org.qcri.rheem.core.optimizer.costs;

import org.junit.Assert;
import org.junit.Test;
import static org.mockito.Mockito.*;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
import org.qcri.rheem.core.plan.OutputSlot;

import java.util.function.ToLongFunction;




/**
 * Test suite for the {@link DefaultCardinalityEstimator}.
 */
public class DefaultCardinalityEstimatorTest {

    @Test
    public void testBinaryInputEstimation() {
        // Mock RheemContext so as to not depend on rheem-basic, which is loaded by default.
        RheemContext rheemContext = mock(RheemContext.class);

        CardinalityEstimate inputEstimate1 = new CardinalityEstimate(50, 60, 0.8);
        CardinalityEstimate inputEstimate2 = new CardinalityEstimate(10, 100, 0.4);

        final ToLongFunction<long[]> singlePointEstimator =
                inputEstimates -> (long) Math.ceil(0.8 * inputEstimates[0] * inputEstimates[1]);

        CardinalityEstimator estimator = new DefaultCardinalityEstimator(
                0.9,
                2,
                singlePointEstimator,
                mock(OutputSlot.class),
                null
        );

        CardinalityEstimate estimate = estimator.estimate(rheemContext, inputEstimate1, inputEstimate2);

        Assert.assertEquals(0.9 * 0.4, estimate.getCorrectnessProbability(), 0.001);
        Assert.assertEquals(singlePointEstimator.applyAsLong(new long[]{50, 10}), estimate.getLowerEstimate());
        Assert.assertEquals(singlePointEstimator.applyAsLong(new long[]{60, 100}), estimate.getUpperEstimate());

    }

}
