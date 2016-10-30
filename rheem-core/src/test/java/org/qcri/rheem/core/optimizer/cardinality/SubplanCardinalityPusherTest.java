package org.qcri.rheem.core.optimizer.cardinality;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.configuration.FunctionalKeyValueProvider;
import org.qcri.rheem.core.api.configuration.KeyValueProvider;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ElementaryOperator;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.plan.rheemplan.Subplan;
import org.qcri.rheem.core.plan.rheemplan.test.TestJoin;
import org.qcri.rheem.core.plan.rheemplan.test.TestMapOperator;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;
import org.qcri.rheem.core.test.MockFactory;
import org.qcri.rheem.core.types.DataSetType;

/**
 * Test suite for {@link SubplanCardinalityPusher}.
 */
public class SubplanCardinalityPusherTest {

    private Job job;

    private Configuration configuration;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        KeyValueProvider<OutputSlot<?>, CardinalityEstimator> estimatorProvider =
                new FunctionalKeyValueProvider<>(
                        (outputSlot, requestee) -> {
                            assert outputSlot.getOwner().isElementary()
                                    : String.format("Cannot provide estimator for composite %s.", outputSlot.getOwner());
                            return ((ElementaryOperator) outputSlot.getOwner())
                                    .createCardinalityEstimator(outputSlot.getIndex(), this.configuration)
                                    .orElse(null);
                        },
                        this.configuration);
        this.configuration.setCardinalityEstimatorProvider(estimatorProvider);
        this.job = MockFactory.createJob(this.configuration);
    }


    @Test
    public void testSimpleSubplan() {
        TestMapOperator<String, String> op1 = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );
        TestMapOperator<String, String> op2 = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );

        op1.connectTo(0, op2, 0);

        Subplan subplan = (Subplan) Subplan.wrap(op1, op2);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(123, 321, 0.123d);
        subplanCtx.setInputCardinality(0, inputCardinality);
        subplan.propagateInputCardinality(0, subplanCtx);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        Assert.assertEquals(inputCardinality, subplanCtx.getOutputCardinality(0));
    }

    @Test
    public void testSourceSubplan() {
        TestSource<String> source = new TestSource<>(DataSetType.createDefault(String.class));
        final CardinalityEstimate sourceCardinality = new CardinalityEstimate(123, 321, 0.123d);
        source.setCardinalityEstimators((optimizationContext, inputEstimates) -> sourceCardinality);

        TestMapOperator<String, String> op = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );

        source.connectTo(0, op, 0);

        Subplan subplan = (Subplan) Subplan.wrap(source, op);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        Assert.assertEquals(sourceCardinality, subplanCtx.getOutputCardinality(0));
    }


    @Test
    public void testDAGShapedSubplan() {
        // _/-\_
        //  \ /
        final DataSetType<String> stringDataSetType = DataSetType.createDefault(String.class);
        TestMapOperator<String, String> map1 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map1.setName("map1");
        TestMapOperator<String, String> map2 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map2.setName("map2");
        TestMapOperator<String, String> map3 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map3.setName("map3");
        TestJoin<String, String, String> join1 = new TestJoin<>(stringDataSetType, stringDataSetType, stringDataSetType);
        join1.setName("join1");
        TestMapOperator<String, String> map4 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        map4.setName("map4");

        map1.connectTo(0, map2, 0);
        map1.connectTo(0, map3, 0);
        map2.connectTo(0, join1, 0);
        map3.connectTo(0, join1, 1);
        join1.connectTo(0, map4, 0);

        Subplan subplan = (Subplan) Subplan.wrap(map1, map4);
        OptimizationContext optimizationContext = new DefaultOptimizationContext(this.job, subplan);
        final OptimizationContext.OperatorContext subplanCtx = optimizationContext.getOperatorContext(subplan);
        final CardinalityEstimate inputCardinality = new CardinalityEstimate(10, 100, 0.9d);
        subplanCtx.setInputCardinality(0, inputCardinality);
        subplan.propagateInputCardinality(0, subplanCtx);

        final CardinalityPusher pusher = SubplanCardinalityPusher.createFor(subplan, this.configuration);
        pusher.push(subplanCtx, this.configuration);

        final CardinalityEstimate outputCardinality = subplanCtx.getOutputCardinality(0);
        final CardinalityEstimate expectedCardinality = new CardinalityEstimate(10 * 10, 100 * 100, 0.9d * 0.7d);
        Assert.assertEquals(expectedCardinality, outputCardinality);
    }

}
