package org.qcri.rheem.spark.channels;

import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;

/**
 * {@link Channel} that represents a broadcasted value.
 */
public class BroadcastChannel extends Channel {

    private static final boolean IS_REUSABLE = true;

    private static final boolean IS_INTERNAL = true;

    protected BroadcastChannel(ExecutionTask producer, int outputIndex, CardinalityEstimate cardinalityEstimate) {
        super(producer, outputIndex, cardinalityEstimate);
    }

    @Override
    public boolean isReusable() {
        return IS_REUSABLE;
    }

    public static class Initializer implements ChannelInitializer {

        @Override
        public Channel setUpOutput(ExecutionTask executionTask, int index) {
            return null;
        }

        @Override
        public void setUpInput(Channel channel, ExecutionTask executionTask, int index) {

        }

        @Override
        public boolean isReusable() {
            return IS_REUSABLE;
        }

        @Override
        public boolean isInternal() {
            return IS_INTERNAL;
        }
    }

}
