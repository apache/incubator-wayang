package org.qcri.rheem.graphchi.operators;

import org.junit.Test;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.graphchi.GraphChiPlatform;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for the {@link GraphChiPageRankOperator}.
 */
public class GraphChiPageRankOperatorTest {

    @Test
    public void testExecution() throws IOException {
        // Ensure that the GraphChiPlatform is initialized.
        GraphChiPlatform.getInstance();

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel inputFile = new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR);
        inputFile.addPath(this.getClass().getResource("/test.edgelist").toString());

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        FileChannel outputFile = new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR);
        final File tempFile = File.createTempFile("rheem-graphchi", "bin");
        tempFile.deleteOnExit();
        outputFile.addPath(tempFile.toURI().toString());

        final GraphChiPageRankOperator graphChiPageRankOperator = new GraphChiPageRankOperator(20);
        graphChiPageRankOperator.execute(new Channel[]{inputFile}, new Channel[]{outputFile});
    }

}
