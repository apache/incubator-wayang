package org.qcri.rheem.java.operators;

import org.junit.BeforeClass;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.java.test.ChannelFactory;

import java.util.Collection;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Superclass for tests of {@link JavaExecutionOperator}s.
 */
public class JavaExecutionOperatorTestBase {

    protected static Configuration configuration;

    @BeforeClass
    public static void init() {
        configuration = new Configuration();
    }

    protected static JavaExecutor createExecutor() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        return new JavaExecutor(JavaPlatform.getInstance(), job);
    }

    protected static OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(configuration);
        return optimizationContext.addOneTimeOperator(operator);
    }

    protected static void evaluate(JavaExecutionOperator operator,
                                   ChannelInstance[] inputs,
                                   ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, createExecutor(), createOperatorContext(operator));
    }

    protected static StreamChannel.Instance createStreamChannelInstance() {
        return ChannelFactory.createStreamChannelInstance(configuration);
    }

    protected static StreamChannel.Instance createStreamChannelInstance(Stream<?> stream) {
        return ChannelFactory.createStreamChannelInstance(stream, configuration);
    }

    protected static CollectionChannel.Instance createCollectionChannelInstance() {
        return ChannelFactory.createCollectionChannelInstance(configuration);
    }

    protected static CollectionChannel.Instance createCollectionChannelInstance(Collection<?> collection) {
        return ChannelFactory.createCollectionChannelInstance(collection, configuration);
    }

}
