package org.apache.wayang.ml.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.junit.jupiter.api.Test;

public class OrtTensorEncoderTest extends JavaExecutionTestBase {

    @Test
    public void testTranspose() {
        final ArrayList<long[][]> input = new ArrayList<>();
        input.add(new long[][] { { 1, 2 }, { 3, 4 } });

        final ArrayList<long[][]> result = OrtTensorEncoder.transpose(input);

        assertEquals(1, result.size());
        assertArrayEquals(new long[] { 1, 3 }, result.get(0)[0]);
        assertArrayEquals(new long[] { 2, 4 }, result.get(0)[1]);
    }
}
