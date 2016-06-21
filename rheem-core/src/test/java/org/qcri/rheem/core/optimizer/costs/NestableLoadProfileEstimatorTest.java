package org.qcri.rheem.core.optimizer.costs;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;

/**
 * Tests for the {@link NestableLoadProfileEstimator}.
 */
public class NestableLoadProfileEstimatorTest {

    @Test
    public void testFromSpecification() {
        String specification = "{" +
                "\"in\":2," +
                "\"out\":1," +
                "\"p\":0.8," +
                "\"cpu\":\"${3*in0 + 2*in1 + 7*out0}\"," +
                "\"ram\":\"${6*in0 + 4*in1 + 14*out0}\"," +
                "\"overhead\":143," +
                "\"ru\":\"${1 - 1/(in0 + in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator = NestableLoadProfileEstimator.parseSpecification(specification);
        final LoadProfile estimate = estimator.estimate(
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)}
        );

        Assert.assertEquals(estimate.getCpuUsage().getLowerEstimate(), 3*10 + 2*100 + 7*200, 0.01);
        Assert.assertEquals(estimate.getCpuUsage().getUpperEstimate(), 3*10 + 2*100 + 7*300, 0.01);
        Assert.assertEquals(estimate.getResourceUtilization(), 1d - 1d/(10+100), 0.000000001);
        Assert.assertEquals(estimate.getOverheadMillis(), 143);
    }
}
