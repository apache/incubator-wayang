package org.qcri.rheem.core.optimizer.costs;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.optimizer.cardinality.CompositeCardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.Subplan;
import org.qcri.rheem.core.plan.rheemplan.test.TestJoin;
import org.qcri.rheem.core.plan.rheemplan.test.TestMapOperator;
import org.qcri.rheem.core.plan.rheemplan.test.TestSource;
import org.qcri.rheem.core.types.DataSetType;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link CompositeCardinalityEstimator}.
 */
public class CompositeCardinalityEstimatorTest {

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
        final CardinalityEstimator estimator = subplan.getCardinalityEstimator(0, mock(Configuration.class)).get();

        final CardinalityEstimate inputEstimate = new CardinalityEstimate(10, 100, 0.9);
        final CardinalityEstimate outputEstimate = estimator.estimate(mock(Configuration.class), inputEstimate);
        Assert.assertEquals(inputEstimate, outputEstimate);
    }

    @Test
    public void testSourceSubplan() {
        TestSource<String> source = new TestSource<>(DataSetType.createDefault(String.class));
        TestMapOperator<String, String> op = new TestMapOperator<>(
                DataSetType.createDefault(String.class),
                DataSetType.createDefault(String.class)
        );

        source.connectTo(0, op, 0);

        Subplan subplan = (Subplan) Subplan.wrap(source, op);
        final CardinalityEstimator estimator = subplan.getCardinalityEstimator(0, mock(Configuration.class)).get();

        final CardinalityEstimate outputEstimate = estimator.estimate(mock(Configuration.class));
        final CardinalityEstimate expectedEstimate = new CardinalityEstimate(100, 100, 1d);
        Assert.assertEquals(expectedEstimate, outputEstimate);
    }


    @Test
    public void testDAGShapedSubplan() {
        // _/-\_
        //  \ /
        final DataSetType<String> stringDataSetType = DataSetType.createDefault(String.class);
        TestMapOperator<String, String> map1 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        TestMapOperator<String, String> map2 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        TestMapOperator<String, String> map3 = new TestMapOperator<>(stringDataSetType, stringDataSetType);
        TestJoin<String, String, String> join1 = new TestJoin<>(stringDataSetType, stringDataSetType, stringDataSetType);
        TestMapOperator<String, String> map4 = new TestMapOperator<>(stringDataSetType, stringDataSetType);

        map1.connectTo(0, map2, 0);
        map1.connectTo(0, map3, 0);
        map2.connectTo(0, join1, 0);
        map2.connectTo(0, join1, 1);
        join1.connectTo(0, map4, 0);

        Subplan subplan = (Subplan) Subplan.wrap(map1, map4);
        final CardinalityEstimator estimator = subplan.getCardinalityEstimator(0, mock(Configuration.class)).get();

        final CardinalityEstimate inputEstimate = new CardinalityEstimate(10, 100, 0.9d);
        final CardinalityEstimate outputEstimate = estimator.estimate(mock(Configuration.class), inputEstimate);
        final CardinalityEstimate expectedEstimate = new CardinalityEstimate(10 * 10, 100 * 100, 0.9d * 0.7d);
        Assert.assertEquals(expectedEstimate, outputEstimate);
    }

}
