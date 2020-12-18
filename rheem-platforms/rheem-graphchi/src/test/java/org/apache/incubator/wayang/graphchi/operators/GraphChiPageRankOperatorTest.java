package io.rheem.rheem.graphchi.operators;

import org.junit.Before;
import org.junit.Test;
import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.optimizer.DefaultOptimizationContext;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.CrossPlatformExecutor;
import io.rheem.rheem.core.profiling.FullInstrumentationStrategy;
import io.rheem.rheem.graphchi.GraphChi;
import io.rheem.rheem.graphchi.execution.GraphChiExecutor;
import io.rheem.rheem.graphchi.platform.GraphChiPlatform;
import io.rheem.rheem.java.channels.StreamChannel;

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

        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));

        final ExecutionOperator outputOperator = mock(ExecutionOperator.class);
        when(outputOperator.getNumOutputs()).thenReturn(1);
        FileChannel.Instance inputChannelInstance =
                (FileChannel.Instance) new FileChannel(FileChannel.HDFS_TSV_DESCRIPTOR)
                        .createInstance(graphChiExecutor, null, -1);
        inputChannelInstance.addPath(this.getClass().getResource("/test.edgelist").toString());
        inputChannelInstance.getLineage().collectAndMark();

        final ExecutionOperator inputOperator = mock(ExecutionOperator.class);
        when(inputOperator.getNumOutputs()).thenReturn(1);
        StreamChannel.Instance outputFileChannelInstance =
                (StreamChannel.Instance) StreamChannel.DESCRIPTOR
                        .createChannel(graphChiPageRankOperator.getOutput(), configuration)
                        .createInstance(graphChiExecutor, null, -1);

        final DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(graphChiPageRankOperator);

        graphChiPageRankOperator.execute(
                new ChannelInstance[]{inputChannelInstance},
                new ChannelInstance[]{outputFileChannelInstance},
                operatorContext
        );
    }

}
