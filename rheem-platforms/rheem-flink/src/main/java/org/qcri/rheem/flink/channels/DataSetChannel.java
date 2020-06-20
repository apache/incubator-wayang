package org.qcri.rheem.flink.channels;

import org.apache.flink.api.java.DataSet;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.AbstractChannelInstance;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.Executor;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.OptionalLong;

/**
 * Describes the situation where one {@link DataSet} is operated on, producing a further {@link DataSet}.
 * <p><i>NB: We might be more specific: Distinguish between cached/uncached and pipelined/aggregated.</i></p>
 */
public class DataSetChannel extends Channel{

    public static final ChannelDescriptor DESCRIPTOR = new ChannelDescriptor(
            DataSetChannel.class, true, false
    );

    public static final ChannelDescriptor DESCRIPTOR_MANY = new ChannelDescriptor(
            DataSetChannel.class, true, false
    );

    public DataSetChannel(ChannelDescriptor descriptor, OutputSlot<?> outputSlot) {
        super(descriptor, outputSlot);
        assert descriptor == DESCRIPTOR || descriptor == DESCRIPTOR_MANY;
        //
        this.markForInstrumentation();
    }

    private DataSetChannel(DataSetChannel parent) {
        super(parent);
    }

    @Override
    public Channel copy() {
        return new DataSetChannel(this);
    }

    @Override
    public Instance createInstance(Executor executor,
                                   OptimizationContext.OperatorContext producerOperatorContext,
                                   int producerOutputIndex) {
        return new Instance((FlinkExecutor) executor, producerOperatorContext, producerOutputIndex);
    }



    /**
     * {@link ChannelInstance} implementation for {@link DataSet}s.
     */
    public class Instance extends AbstractChannelInstance {

        private DataSet<?> dataSet;

        private long size;

        public Instance(FlinkExecutor executor,
                        OptimizationContext.OperatorContext producerOperatorContext,
                        int producerOutputIndex) {
            super(executor, producerOperatorContext, producerOutputIndex);
        }

        public void accept(DataSet dataSet, FlinkExecutor flinkExecutor) {
            this.dataSet = dataSet;
        }


        @SuppressWarnings("unchecked")
        public <T> DataSet<T> provideDataSet() {
            return (DataSet<T>) this.dataSet;
        }

        @Override
        protected void doDispose() {
            this.dataSet = null;
        }

        @Override
        public OptionalLong getMeasuredCardinality() {
            return this.size == 0 ? super.getMeasuredCardinality() : OptionalLong.of(this.size);
        }

        @Override
        public DataSetChannel getChannel() {
            return DataSetChannel.this;
        }

    }

}
