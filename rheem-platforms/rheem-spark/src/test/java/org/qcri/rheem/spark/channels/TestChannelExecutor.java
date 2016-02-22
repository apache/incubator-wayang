package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.api.exception.RheemException;

/**
 * {@link ChannelExecutor} implementation for test purposes.
 */
public class TestChannelExecutor implements ChannelExecutor {

    private JavaRDD<?> rdd;

    private Broadcast<?> broadcast;

    public TestChannelExecutor(JavaRDD<?> rdd) {
        this();
        this.rdd = rdd;
    }

    public TestChannelExecutor() {

    }

    @Override
    public void acceptRdd(JavaRDD<?> rdd) throws RheemException {
        this.rdd = rdd;
    }

    @Override
    public void acceptBroadcast(Broadcast broadcast) {
        this.broadcast = broadcast;
    }

    @Override
    @SuppressWarnings("unchecked")
    public JavaRDD<?> provideRdd() {
        return this.rdd;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Broadcast<?> provideBroadcast() {
        return this.broadcast;
    }

    @Override
    public void dispose() {
        // Meh.
    }

    @Override
    public long getCardinality() throws RheemException {
        return -1; // Meh.
    }

    @Override
    public boolean ensureExecution() {
        return false; // Meh.
    }
}
