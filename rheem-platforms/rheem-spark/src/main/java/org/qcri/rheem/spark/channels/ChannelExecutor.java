package org.qcri.rheem.spark.channels;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.slf4j.LoggerFactory;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface ChannelExecutor {

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param rdd the producer's result
     * @throws RheemException if the instance does not accept {@link JavaRDD}s
     */
    void acceptRdd(JavaRDD<?> rdd) throws RheemException;

    /**
     * Accept the result of the producer of a {@link Channel}.
     *
     * @param broadcast the producer's result
     * @throws RheemException if the instance does not accept {@link Broadcast}s
     */
    void acceptBroadcast(Broadcast broadcast);

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    <T> JavaRDD<T> provideRdd();

    /**
     * Provide the producer's result to a consumer.
     *
     * @return the producer's result
     */
    <T> Broadcast<T> provideBroadcast();

    /**
     * Releases resources held by this instance.
     */
    void dispose();

    /**
     * {@link ChannelExecutor} implementation for {@link JavaRDD}s.
     */
    class ForRDD implements ChannelExecutor {

        private JavaRDD<?> rdd;

        private final boolean isCaching;

        public ForRDD(boolean isCaching) {
            this.isCaching = isCaching;
        }

        @Override
        public void acceptRdd(JavaRDD<?> rdd) throws RheemException {
            this.rdd = rdd;
            if (this.isCaching) {
                this.rdd.cache();
            }
        }

        @Override
        public void acceptBroadcast(Broadcast broadcast) {
            throw new RheemException("Wrong channel for broadcasts.");
        }

        @Override
        @SuppressWarnings("unchecked")
        public JavaRDD<?> provideRdd() {
            return this.rdd;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Broadcast<?> provideBroadcast() {
            throw new RheemException("Wrong channel for broadcasts.");
        }

        @Override
        public void dispose() {
            if (this.isCaching && this.rdd != null) {
                try {
                    this.rdd.unpersist();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).warn("Unpersisting RDD failed.", t);
                }
            }
        }
    }

    /**
     * {@link ChannelExecutor} implementation for {@link Broadcast}s.
     */
    class ForBroadcast implements ChannelExecutor {

        private Broadcast<?> broadcast;

        @Override
        public void acceptRdd(JavaRDD<?> rdd) throws RheemException {
            throw new RheemException("Wrong channel for RDDs.");
        }

        @Override
        public void acceptBroadcast(Broadcast broadcast) {
            this.broadcast = broadcast;
        }

        @Override
        @SuppressWarnings("unchecked")
        public JavaRDD<?> provideRdd() {
            throw new RheemException("Wrong channel for RDDs.");
        }

        @Override
        @SuppressWarnings("unchecked")
        public Broadcast<?> provideBroadcast() {
            return this.broadcast;
        }

        @Override
        public void dispose() {
            if (this.broadcast != null) {
                try {
                    this.broadcast.destroy(false);
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).warn("Destroying broadcast failed.", t);
                }
            }
        }
    }
}
