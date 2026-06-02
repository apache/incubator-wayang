package org.apache.wayang.ml.test;

import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.ml.encoding.OneHotVector;
import org.junit.jupiter.api.Test;

public class OneHotVectorTest {
    @Test
    public void testOneHotVector() {
        final OneHotVector vector = new OneHotVector();
        final long[] encoded = new long[12];
        final LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        vector.addOperator(encoded, sink.getClass().getName());
    }
}
