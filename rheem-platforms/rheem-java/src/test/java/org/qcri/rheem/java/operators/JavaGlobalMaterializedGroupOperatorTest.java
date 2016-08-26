package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Test suite for {@link JavaGlobalReduceOperator}.
 */
public class JavaGlobalMaterializedGroupOperatorTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        // Prepare test data.
        Collection<Integer> inputCollection = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Build the reduce operator.
        JavaGlobalMaterializedGroupOperator<Integer> globalGroup =
                new JavaGlobalMaterializedGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createGroupedUnchecked(Iterable.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createCollectionChannelInstance(inputCollection)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};
        evaluate(globalGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Collection<Integer>> result = ((CollectionChannel.Instance) outputs[0]).provideCollection();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(inputCollection, result.iterator().next());

    }

    @Test
    public void testExecutionWithoutData() {
        // Prepare test data.
        Collection<Integer> inputCollection = Collections.emptyList();

        // Build the reduce operator.
        JavaGlobalMaterializedGroupOperator<Integer> globalGroup =
                new JavaGlobalMaterializedGroupOperator<>(
                        DataSetType.createDefaultUnchecked(Integer.class),
                        DataSetType.createGroupedUnchecked(Iterable.class)
                );

        // Execute.
        JavaChannelInstance[] inputs = new JavaChannelInstance[]{createCollectionChannelInstance(inputCollection)};
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};
        evaluate(globalGroup, inputs, outputs);

        // Verify the outcome.
        final Collection<Collection<Integer>> result = ((CollectionChannel.Instance) outputs[0]).provideCollection();
        Assert.assertEquals(1, result.size());
        Assert.assertTrue(result.iterator().next().isEmpty());
    }
}
