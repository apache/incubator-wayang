package org.qcri.rheem.spark.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sets up {@link HdfsFile} usage in the {@link SparkPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer<HdfsFile> {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public HdfsFile setUpOutput(ExecutionTask executionTask, int index) {
        // TODO: Implement correctly.
        this.logger.warn("HdfsFiles are not support, yet.");
        return new HdfsFile(executionTask, index);

    }

    @Override
    public void setUpInput(HdfsFile channel, ExecutionTask executionTask, int index) {
        // TODO: Implement correctly.
        this.logger.warn("HdfsFiles are not support, yet.");
        channel.addConsumer(executionTask, index);
    }

    @Override
    public boolean isReusable() {
        return true;
    }
}
