package org.qcri.rheem.java.channels;

import org.qcri.rheem.basic.channels.HdfsFile;
import org.qcri.rheem.core.plan.executionplan.ChannelInitializer;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.java.plugin.JavaPlatform;

/**
 * Sets up {@link HdfsFile} usage in the {@link JavaPlatform}.
 */
public class HdfsFileInitializer implements ChannelInitializer<HdfsFile> {

    @Override
    public HdfsFile setUpOutput(ExecutionTask executionTask, int index) {
        throw new RuntimeException("Not implemented yet.");
    }

    @Override
    public void setUpInput(HdfsFile channel, ExecutionTask executionTask, int index) {
        throw new RuntimeException("Not implemented yet.");
    }

    @Override
    public boolean isReusable() {
        return true;
    }
}
