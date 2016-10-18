package org.qcri.rheem.graphchi.operators;

import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.graphchi.GraphChi;
import org.qcri.rheem.graphchi.execution.GraphChiExecutor;
import org.qcri.rheem.graphchi.platform.GraphChiPlatform;
import org.qcri.rheem.java.channels.StreamChannel;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test suite for the {@link GraphChiPageRankOperator}.
 */
public class GraphChiPageRankOperatorTest {

    private static GraphChiExecutor graphChiExecutor;

    @Before
    public void setUp() {
        graphChiExecutor = mock(GraphChiExecutor.class);
    }

    @Test
    public void testExecution() throws IOException {
        // Ensure that the GraphChiPlatform is initialized.
        GraphChiPlatform.getInstance();
        final Configuration configuration = new Configuration();
        GraphChi.plugin().configure(configuration);
        final GraphChiPageRankOperator graphChiPageRankOperator = new GraphChiPageRankOperator(20);

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel.Instance inputChannelInstance =
                (FileChannel.Instance) new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR)
                        .createInstance(graphChiExecutor, null, -1);
        inputChannelInstance.addPath(this.getClass().getResource("/test.edgelist").toString());

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        StreamChannel.Instance outputFileChannelInstance =
                (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                        .createChannel(graphChiPageRankOperator.getOutput(), configuration)
                        .createInstance(graphChiExecutor, null, -1);

        graphChiPageRankOperator.execute(
                new ChannelInstance[]{inputChannelInstance},
                new ChannelInstance[]{outputFileChannelInstance},
                configuration
        );
    }

}
