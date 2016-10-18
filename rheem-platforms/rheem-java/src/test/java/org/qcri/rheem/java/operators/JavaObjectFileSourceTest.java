package org.qcri.rheem.java.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.execution.JavaExecutor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test suite for {@link JavaObjectFileSource}.
 */
public class JavaObjectFileSourceTest extends JavaExecutionOperatorTestBase {

    @Test
    public void testReading() throws IOException {
        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = this.getClass().getResource("/0-to-10000.sequence_file");
            JavaObjectFileSource<Integer> source = new JavaObjectFileSource<>(
                    inputUrl.toString(), DataSetType.createDefault(Integer.class));


            // Execute.
            JavaChannelInstance[] inputs = new JavaChannelInstance[]{};
            JavaChannelInstance[] outputs = new JavaChannelInstance[]{createStreamChannelInstance()};
            evaluate(source, inputs, outputs);

            // Verify.
            Set<Integer> expectedValues = new HashSet<>(JavaObjectFileSourceTest.enumerateRange(10000));
            // Verify the outcome.
            final List<Integer> result = outputs[0].<Integer>provideStream()
                    .collect(Collectors.toList());
            for (Object value : result) {
                Assert.assertTrue("Value: " + value, expectedValues.remove(value));
            }
            Assert.assertEquals(0, expectedValues.size());
        } finally {
            if (javaExecutor != null) javaExecutor.dispose();
        }

    }

    private static List<Integer> enumerateRange(int to) {
        Validate.isTrue(to >= 0);
        List<Integer> range = new ArrayList<>(to);
        for (int i = 0; i < to; i++) {
            range.add(i);
        }
        return range;
    }
}
