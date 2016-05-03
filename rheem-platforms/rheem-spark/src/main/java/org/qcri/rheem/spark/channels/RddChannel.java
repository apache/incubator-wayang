package org.qcri.rheem.spark.channels;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.spark.platform.SparkExecutor;

import java.util.OptionalLong;

/**
 * Describes the situation where one {@link JavaRDD} is operated on, producing a further {@link JavaRDD}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class RddChannel extends Channel {

    private static final boolean IS_UNCACHED_REUSABLE = true;

    private static final boolean IS_CACHED_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    public static final ChannelDescriptor UNCACHED_DESCRIPTOR = new ChannelDescriptor(
            RddChannel.class, IS_UNCACHED_REUSABLE, IS_UNCACHED_REUSABLE, !IS_INTERNAL && IS_UNCACHED_REUSABLE
    );

    public static final ChannelDescriptor CACHED_DESCRIPTOR = new ChannelDescriptor(
            RddChannel.class, IS_CACHED_REUSABLE, IS_CACHED_REUSABLE, !IS_INTERNAL && IS_CACHED_REUSABLE
    );

    public RddChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == UNCACHED_DESCRIPTOR || descriptor == CACHED_DESCRIPTOR;
    }

    private RddChannel(RddChannel parent) {
        super(parent);
    }

    @Override
    public RddChannel copy() {
        return new RddChannel(this);
    }

    @Override
    public ChannelInstance createInstance() {
        return new Instance();
    }

    /**
     * {@link ChannelInstance} implementation for {@link RddChannel}s.
     */
    public class Instance extends AbstractChannelInstance {

        private JavaRDD<?> rdd;

        private Accumulator<Integer> accumulator;

        public void accept(JavaRDD<?> rdd, SparkExecutor sparkExecutor) throws RheemException {
            if (this.isMarkedForInstrumentation()) {
                final Accumulator<Integer> accumulator = sparkExecutor.sc.accumulator(0);
                this.rdd = rdd.filter(dataQuantum -> {
                    accumulator.add(1);
                    return true;
                });
                this.accumulator = accumulator;
            } else {
                this.rdd = rdd;
            }
        }


        @SuppressWarnings("unchecked")
        public <T> JavaRDD<T> provideRdd() {
            return (JavaRDD<T>) this.rdd;
        }

        @Override
        public void tryToRelease() {
            if (this.accumulator != null) {
                this.accumulator = null;
            }
            if (this.isRddCached() && this.rdd != null) {
                this.rdd.unpersist();
            }
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            if (this.accumulator != null) {
                this.setMeasuredCardinality(this.accumulator.value());
            }
            return super.getMeasuredCardinality();
        }

        @Override
        public RddChannel getChannel() {
            return RddChannel.this;
        }

        private boolean isRddCached() {
            return this.getChannel().isReusable();
        }
    }

}
