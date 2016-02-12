package org.qcri.rheem.java.operators;

import org.apache.commons.lang3.Validate;
import org.junit.Assert;
import org.junit.Test;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.platform.JavaExecutor;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

/**
 * Test suite for {@link JavaObjectFileSource}.
 */
public class JavaObjectFileSourceTest {

    @Test
    public void testReading() throws IOException {
        JavaExecutor javaExecutor = null;
        try {
            // Prepare the source.
            final URL inputUrl = this.getClass().getResource("/0-to-10000.sequence_file");
            JavaObjectFileSource<Integer> source = new JavaObjectFileSource<>(
                    inputUrl.toString(), DataSetType.createDefault(Integer.class));

            // Execute.
            final Stream[] outputStreams = source.evaluate(new Stream[0], mock(FunctionCompiler.class));

            // Verify.
            Assert.assertTrue(outputStreams.length == 1);

            Set<Integer> expectedValues = new HashSet<>(JavaObjectFileSourceTest.enumerateRange(10000));
            final List rddList = (List<Integer>) outputStreams[0].collect(Collectors.toList());
            for (Object rddValue : rddList) {
                Assert.assertTrue("Value: " + rddValue, expectedValues.remove(rddValue));
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
