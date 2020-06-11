package org.qcri.rheem.giraph.operators;

import org.junit.Before;
import org.junit.Test;
import org.qcri.rheem.basic.channels.FileChannel;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.profiling.FullInstrumentationStrategy;
import org.qcri.rheem.giraph.Giraph;
import org.qcri.rheem.giraph.execution.GiraphExecutor;
import org.qcri.rheem.giraph.platform.GiraphPlatform;
import org.qcri.rheem.java.channels.StreamChannel;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test For GiraphPageRank
 */
public class GiraphPagaRankOperatorTest {

    private static GiraphExecutor giraphExecutor;

    @Before
    public void setUp() {
        giraphExecutor = mock(GiraphExecutor.class);
    }

    //TODO Validate the mock of GiraphExecutor @Test
    public void testExecution() throws IOException {
        // Ensure that the GraphChiPlatform is initialized.
        GiraphPlatform.getInstance();
        final Configuration configuration = new Configuration();
        Giraph.plugin().configure(configuration);
        final GiraphPageRankOperator giraphPageRankOperator = new GiraphPageRankOperator(20);

        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel.Instance inputChannelInstance =
                (FileChannel.Instance) new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR)
                        .createInstance(giraphExecutor, null, -1);
        inputChannelInstance.addPath(this.getClass().getResource("/test.edgelist").toString());
        inputChannelInstance.getLineage().collectAndMark();

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        StreamChannel.Instance outputFileChannelInstance =
                (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                        .createChannel(giraphPageRankOperator.getOutput(), configuration)
                        .createInstance(giraphExecutor, null, -1);

        final DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(giraphPageRankOperator);

        giraphPageRankOperator.execute(
                new ChannelInstance[]{inputChannelInstance},
                new ChannelInstance[]{outputFileChannelInstance},
                giraphExecutor,
                operatorContext
        );
    }
}
