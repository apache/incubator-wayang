package org.qcri.rheem.core.optimizer.costs;

import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.UnaryToUnaryOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.types.DataSetType;

import java.util.List;

/**
 * Tests for the {@link NestableLoadProfileEstimator}.
 */
public class NestableLoadProfileEstimatorTest {

    @Test
    public void testFromJuelSpecification() {
        String specification = "{" +
                "\"type\":\"juel\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"p\":0.8," +
                "\"cpu\":\"${3*in0 + 2*in1 + 7*out0}\"," +
                "\"ram\":\"${6*in0 + 4*in1 + 14*out0}\"," +
                "\"overhead\":143," +
                "\"ru\":\"${rheem:logGrowth(0.1, 0.1, 10000, in0+in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator =
                LoadProfileEstimators.createFromSpecification(null, specification);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)},
                new TObjectDoubleHashMap<>(),
                1
        ));

        Assert.assertEquals(3 * 10 + 2 * 100 + 7 * 200, estimate.getCpuUsage().getLowerEstimate(), 0.01);
        Assert.assertEquals(3 * 10 + 2 * 100 + 7 * 300, estimate.getCpuUsage().getUpperEstimate(), 0.01);
        Assert.assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001
        );
        Assert.assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    public void testFromMathExSpecification() {
        String specification = "{" +
                "\"type\":\"mathex\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"p\":0.8," +
                "\"cpu\":\"3*in0 + 2*in1 + 7*out0\"," +
                "\"ram\":\"6*in0 + 4*in1 + 14*out0\"," +
                "\"overhead\":143," +
                "\"ru\":\"logGrowth(0.1, 0.1, 10000, in0+in1)\"" +
                "}";
        final NestableLoadProfileEstimator estimator =
                LoadProfileEstimators.createFromSpecification(null, specification);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)},
                new TObjectDoubleHashMap<>(),
                1
        ));

        Assert.assertEquals(3 * 10 + 2 * 100 + 7 * 200, estimate.getCpuUsage().getLowerEstimate(), 0.01);
        Assert.assertEquals(3 * 10 + 2 * 100 + 7 * 300, estimate.getCpuUsage().getUpperEstimate(), 0.01);
        Assert.assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001
        );
        Assert.assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    public void testFromJuelSpecificationWithImport() {
        String specification = "{" +
                "\"in\":2," +
                "\"out\":1," +
                "\"import\":[\"numIterations\"]," +
                "\"p\":0.8," +
                "\"cpu\":\"${(3*in0 + 2*in1 + 7*out0) * numIterations}\"," +
                "\"ram\":\"${6*in0 + 4*in1 + 14*out0}\"," +
                "\"overhead\":143," +
                "\"ru\":\"${rheem:logGrowth(0.1, 0.1, 10000, in0+in1)}\"" +
                "}";
        final NestableLoadProfileEstimator estimator =
                LoadProfileEstimators.createFromSpecification(null, specification);
        SomeExecutionOperator execOp = new SomeExecutionOperator();
        TObjectDoubleMap<String> properties = new TObjectDoubleHashMap<>();
        properties.put("numIterations", 2d);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)},
                properties,
                1
        ));

        Assert.assertEquals((3 * 10 + 2 * 100 + 7 * 200)  * execOp.getNumIterations(), estimate.getCpuUsage().getLowerEstimate(), 0.01);
        Assert.assertEquals((3 * 10 + 2 * 100 + 7 * 300)  * execOp.getNumIterations(), estimate.getCpuUsage().getUpperEstimate(), 0.01);
        Assert.assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001
        );
        Assert.assertEquals(143, estimate.getOverheadMillis());
    }

    @Test
    public void testMathExFromSpecificationWithImport() {
        String specification = "{" +
                "\"type\":\"mathex\"," +
                "\"in\":2," +
                "\"out\":1," +
                "\"import\":[\"numIterations\"]," +
                "\"p\":0.8," +
                "\"cpu\":\"(3*in0 + 2*in1 + 7*out0) * numIterations\"," +
                "\"ram\":\"6*in0 + 4*in1 + 14*out0\"," +
                "\"overhead\":143," +
                "\"ru\":\"logGrowth(0.1, 0.1, 10000, in0+in1)\"" +
                "}";
        final NestableLoadProfileEstimator estimator =
                LoadProfileEstimators.createFromSpecification(null, specification);
        SomeExecutionOperator execOp = new SomeExecutionOperator();
        TObjectDoubleMap<String> properties = new TObjectDoubleHashMap<>();
        properties.put("numIterations", 2d);
        final LoadProfile estimate = estimator.estimate(new SimpleEstimationContext(
                new CardinalityEstimate[]{
                        new CardinalityEstimate(10, 10, 1d), new CardinalityEstimate(100, 100, 1d)
                },
                new CardinalityEstimate[]{new CardinalityEstimate(200, 300, 1d)},
                properties,
                1
        ));

        Assert.assertEquals((3 * 10 + 2 * 100 + 7 * 200)  * execOp.getNumIterations(), estimate.getCpuUsage().getLowerEstimate(), 0.01);
        Assert.assertEquals((3 * 10 + 2 * 100 + 7 * 300)  * execOp.getNumIterations(), estimate.getCpuUsage().getUpperEstimate(), 0.01);
        Assert.assertEquals(
                OptimizationUtils.logisticGrowth(0.1, 0.1, 10000, 100 + 10),
                estimate.getResourceUtilization(),
                0.000000001
        );
        Assert.assertEquals(143, estimate.getOverheadMillis());
    }

    public static class SomeOperator extends UnaryToUnaryOperator<Object, Object> {

        public int getNumIterations() {
            return 2;
        }

        protected SomeOperator() {
            super(DataSetType.createDefault(Object.class), DataSetType.createDefault(Object.class), false);
        }
    }

    public static class SomeExecutionOperator extends SomeOperator implements ExecutionOperator {

        @Override
        public Platform getPlatform() {
            return null;
        }

        @Override
        public List<ChannelDescriptor> getSupportedInputChannels(int index) {
            return null;
        }

        @Override
        public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
            return null;
        }

    }
}
