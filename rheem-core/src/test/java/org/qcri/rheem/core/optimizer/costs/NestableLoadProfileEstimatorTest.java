package org.qcri.rheem.core.optimizer.costs;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
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
                "\"ru\":\"${rheem:logGrowth(0.1, 0.1, 10000, in0+in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator = LoadProfileEstimators.createFromJuelSpecification(specification);
        final LoadProfile estimate = estimator.estimate(
                null,
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)}
        );

        Assert.assertEquals(3*10 + 2*100 + 7*200, estimate.getCpuUsage().getLowerEstimate(), 0.01);
        Assert.assertEquals(3*10 + 2*100 + 7*300, estimate.getCpuUsage().getUpperEstimate(), 0.01);
        Assert.assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001
        );
        Assert.assertEquals(143, estimate.getOverheadMillis());
    }
}
