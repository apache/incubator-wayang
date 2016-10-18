package org.qcri.rheem.java.operators;

import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test suite for the {@link JavaCollectionSource}.
 */
public class JavaCollectionSourceTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testExecution() {
        Set<Integer> inputValues = new HashSet<>(Arrays.asList(1, 2, 3));
        JavaCollectionSource collectionSource = new JavaCollectionSource(
                inputValues,
                DataSetType.createDefault(Integer.class));
        JavaChannelInstance[] inputs = new JavaChannelInstance[0];
        JavaChannelInstance[] outputs = new JavaChannelInstance[]{createCollectionChannelInstance()};

        evaluate(collectionSource, inputs, outputs);

        final Set<Object> outputValues = outputs[0].provideStream().collect(Collectors.toSet());
        Assert.assertEquals(outputValues, inputValues);
    }


}
