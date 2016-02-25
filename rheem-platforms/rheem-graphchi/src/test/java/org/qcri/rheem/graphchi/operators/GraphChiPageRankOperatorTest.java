package org.qcri.rheem.graphchi.operators;

import org.junit.Test;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.graphchi.GraphChiPlatform;
import org.qcri.rheem.graphchi.channels.ChannelManager;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for the {@link GraphChiPageRankOperator}.
 */
public class GraphChiPageRankOperatorTest {

    @Test(expected = AssertionError.class) // GraphChi throws an AssertionError, although we are using it correctly.
    public void testExecution() throws IOException {
        GraphChiPlatform.getInstance();

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel inputFile = new FileChannel(ChannelManager.HDFS_TSV_DESCRIPTOR, new ExecutionTask(outputOperator), 0, null);
        inputFile.addPath(this.getClass().getResource("/test.edgelist").toString());

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        FileChannel outputFile = new FileChannel(ChannelManager.HDFS_TSV_DESCRIPTOR, new ExecutionTask(inputOperator), 0, null);
        final File tempFile = File.createTempFile("rheem-graphchi", "bin");
        tempFile.deleteOnExit();
        outputFile.addPath(tempFile.toURI().toString());

        final GraphChiPageRankOperator graphChiPageRankOperator = new GraphChiPageRankOperator();
        graphChiPageRankOperator.execute(new Channel[]{inputFile}, new Channel[]{outputFile});
    }

}
