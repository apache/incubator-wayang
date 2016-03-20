package org.qcri.rheem.spark.channels;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.operators.SparkBroadcastOperator;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.slf4j.LoggerFactory;

import java.util.OptionalLong;

/**
 * Defines execution logic to handle a {@link Channel}.
 */
public interface ChannelExecutor extends ChannelInstance {

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
     * Request this instance to pull the data for its {@link Channel} if this has not happened yet.
     *
     * @return whether the execution really took place
     */
    boolean ensureExecution();

    /**
     * {@link ChannelExecutor} implementation for {@link JavaRDD}s.
     */
    class ForRDD implements ChannelExecutor {

        private final SparkExecutor sparkExecutor;

        private final RddChannel channel;

        private JavaRDD<?> rdd;

        private final boolean isCaching;

        private Accumulator<Integer> accumulator;

        public ForRDD(Channel channel, boolean isCaching, SparkExecutor sparkExecutor) {
            this.channel = (RddChannel) channel;
            this.isCaching = isCaching;
            this.sparkExecutor = sparkExecutor;
        }

        @Override
        public void acceptRdd(JavaRDD<?> rdd) throws RheemException {
            if (this.channel.isMarkedForInstrumentation()) {
                final Accumulator<Integer> accumulator = this.sparkExecutor.sc.accumulator(0);
                this.rdd = rdd.filter(dataQuantum -> {
                    accumulator.add(1);
                    return true;
                });
                this.accumulator = accumulator;
            } else {
                this.rdd = rdd;
            }
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
        public void release() {
            if (this.isCaching && this.rdd != null) {
                try {
                    this.rdd.unpersist();
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).warn("Unpersisting RDD failed.", t);
                }
            }
        }

        @Override
        public OptionalLong getMeasuredCardinality() throws RheemException {
            if (!this.channel.isMarkedForInstrumentation()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(this.accumulator.value());
        }

        @Override
        public boolean ensureExecution() {
            // TODO: This right-here is blunt.
            LoggerFactory.getLogger(this.getClass()).warn("Bluntly forcing execution on {}.", this.rdd);
            assert this.rdd != null : String.format("RDD missing for %s.", this.channel);
            this.rdd.cache().foreachPartition((i) -> {});
            return true;
        }

        @Override
        public RddChannel getChannel() {
            return this.channel;
        }
    }


    /**
     * {@link ChannelExecutor} implementation for {@link Broadcast}s.
     */
    class ForBroadcast implements ChannelExecutor {

        private final BroadcastChannel channel;

        private Broadcast<?> broadcast;

        public ForBroadcast(Channel channel) {
            this.channel = (BroadcastChannel) channel;
        }

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
        public void release() {
            if (this.broadcast != null) {
                try {
                    this.broadcast.destroy(false);
                } catch (Throwable t) {
                    LoggerFactory.getLogger(this.getClass()).warn("Destroying broadcast failed.", t);
                }
            }
        }

        @Override
        public OptionalLong getMeasuredCardinality() throws RheemException {
            return ((SparkBroadcastOperator<?>) this.channel.getProducer().getOperator()).getMeasuredCardinality();
        }

        @Override
        public boolean ensureExecution() {
            return this.broadcast != null;
        }

        @Override
        public BroadcastChannel getChannel() {
            return this.channel;
        }
    }
}
