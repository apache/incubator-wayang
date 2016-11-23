package org.qcri.rheem.core.platform;

import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.configuration.KeyValueProvider;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.test.DummyExecutionOperator;
import org.qcri.rheem.core.test.DummyPlatform;
import org.qcri.rheem.core.test.SerializableDummyExecutionOperator;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suites for {@link PartialExecution}s.
 */
public class PartialExecutionTest {

    @Test
    public void testJsonSerialization() {
        // Create first OperatorContext with non-serializable ExecutionOperator.
        final OptimizationContext.OperatorContext operatorContext1 = mock(OptimizationContext.OperatorContext.class);
        when(operatorContext1.getOperator()).thenReturn(new DummyExecutionOperator(1, 1, false));
        when(operatorContext1.getInputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext1.getOutputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext1.getNumExecutions()).thenReturn(1);

        // Create first OperatorContext with non-serializable ExecutionOperator.
        final OptimizationContext.OperatorContext operatorContext2 = mock(OptimizationContext.OperatorContext.class);
        when(operatorContext2.getOperator()).thenReturn(new SerializableDummyExecutionOperator(52));
        when(operatorContext2.getInputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext2.getOutputCardinalities()).thenReturn(new CardinalityEstimate[]{new CardinalityEstimate(23, 42, 0.5)});
        when(operatorContext2.getNumExecutions()).thenReturn(1);

        Configuration configuration = new Configuration();
        final KeyValueProvider<ExecutionOperator, LoadProfileEstimator> estimatorProvider = configuration.getOperatorLoadProfileEstimatorProvider();
        ExecutionLineageNode executionLineageNode1 = new ExecutionLineageNode(operatorContext1)
                .add(estimatorProvider.provideFor((ExecutionOperator) operatorContext1.getOperator()));
        ExecutionLineageNode executionLineageNode2 = new ExecutionLineageNode(operatorContext1)
                .add(estimatorProvider.provideFor((ExecutionOperator) operatorContext2.getOperator()));
        PartialExecution original = new PartialExecution(12345L, 12, 13,
                Arrays.asList(executionLineageNode1, executionLineageNode2),
                configuration
        );
        original.addInitializedPlatform(DummyPlatform.getInstance());

        final PartialExecution.Serializer serializer = new PartialExecution.Serializer(configuration);
        final JSONObject jsonObject = JsonSerializables.serialize(original, false, serializer);
        final PartialExecution loaded = JsonSerializables.deserialize(jsonObject, serializer, PartialExecution.class);

        Assert.assertEquals(original.getMeasuredExecutionTime(), loaded.getMeasuredExecutionTime());
        Assert.assertEquals(2, loaded.getAtomicExecutionGroups().size());
        Assert.assertEquals(1, loaded.getInitializedPlatforms().size());
        Assert.assertSame(DummyPlatform.getInstance(), RheemCollections.getAny(loaded.getInitializedPlatforms()));
    }

}