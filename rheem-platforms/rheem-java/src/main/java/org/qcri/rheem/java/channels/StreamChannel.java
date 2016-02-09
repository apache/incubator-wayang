package org.qcri.rheem.java.channels;

import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.stream.Stream;

/**
 * {@link Channel} between two {@link JavaExecutionOperator}s using a {@link Stream}.
 */
public class StreamChannel extends Channel {

    protected StreamChannel(ExecutionTask producer, int outputIndex) {
        super(producer, outputIndex);
    }

    @Override
    public boolean isReusable() {
        return false;
    }

    public static class Initializer implements ChannelInitializer<StreamChannel> {

        @Override
        public StreamChannel setUpOutput(ExecutionTask executionTask, int index) {
            return new StreamChannel(executionTask, index);
        }

        @Override
        public void setUpInput(StreamChannel collectionChannel, ExecutionTask executionTask, int index) {
            collectionChannel.addConsumer(executionTask, index);
        }

        @Override
        public boolean isReusable() {
            return false;
        }
    }
}
