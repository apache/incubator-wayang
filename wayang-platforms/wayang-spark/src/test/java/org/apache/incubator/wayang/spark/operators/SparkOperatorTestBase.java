package org.apache.incubator.wayang.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.api.Job;
import org.apache.incubator.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.incubator.wayang.core.optimizer.OptimizationContext;
import org.apache.incubator.wayang.core.plan.wayangplan.Operator;
import org.apache.incubator.wayang.core.platform.ChannelInstance;
import org.apache.incubator.wayang.core.platform.CrossPlatformExecutor;
import org.apache.incubator.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.incubator.wayang.java.channels.CollectionChannel;
import org.apache.incubator.wayang.spark.channels.RddChannel;
import org.apache.incubator.wayang.spark.execution.SparkExecutor;
import org.apache.incubator.wayang.spark.platform.SparkPlatform;
import org.apache.incubator.wayang.spark.test.ChannelFactory;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test base for {@link SparkExecutionOperator} tests.
 */
public class SparkOperatorTestBase {

    protected Configuration configuration;

    protected SparkExecutor sparkExecutor;

    @Before
    public void setUp() {
        this.configuration = new Configuration();
        this.sparkExecutor = (SparkExecutor) SparkPlatform.getInstance().getExecutorFactory().create(this.mockJob());
    }

    Job mockJob() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(this.configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));
        return job;
    }

    protected OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(mockJob());
        return optimizationContext.addOneTimeOperator(operator);
    }

    protected void evaluate(SparkExecutionOperator operator,
                            ChannelInstance[] inputs,
                            ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, this.sparkExecutor, this.createOperatorContext(operator));
    }

    RddChannel.Instance createRddChannelInstance() {
        return ChannelFactory.createRddChannelInstance(this.configuration);
    }

    RddChannel.Instance createRddChannelInstance(Collection<?> collection) {
        return ChannelFactory.createRddChannelInstance(collection, this.sparkExecutor, this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance() {
        return ChannelFactory.createCollectionChannelInstance(this.configuration);
    }

    protected CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        return ChannelFactory.createCollectionChannelInstance(collection, this.configuration);
    }

    public JavaSparkContext getSC() {
        return this.sparkExecutor.sc;
    }

}
