package io.rheem.rheem.java.operators;

import org.junit.BeforeClass;
import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.api.Job;
import io.rheem.rheem.core.optimizer.DefaultOptimizationContext;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.optimizer.cardinality.CardinalityEstimate;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.CrossPlatformExecutor;
import io.rheem.rheem.core.profiling.NoInstrumentationStrategy;
import io.rheem.rheem.java.channels.CollectionChannel;
import io.rheem.rheem.java.channels.StreamChannel;
import io.rheem.rheem.java.execution.JavaExecutor;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.java.test.ChannelFactory;

import java.util.Collection;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Superclass for tests of {@link JavaExecutionOperator}s.
 */
public class JavaExecutionOperatorTestBase {

    protected static Configuration configuration;

    protected static Job job;

    @BeforeClass
    public static void init() {
        configuration = new Configuration();
        job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        DefaultOptimizationContext optimizationContext = new DefaultOptimizationContext(job);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new NoInstrumentationStrategy()));
        when(job.getOptimizationContext()).thenReturn(optimizationContext);
    }

    protected static JavaExecutor createExecutor() {
        return new JavaExecutor(JavaPlatform.getInstance(), job);
    }

    protected static OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = job.getOptimizationContext();
        final OptimizationContext.OperatorContext operatorContext = optimizationContext.addOneTimeOperator(operator);
        for (int i = 0; i < operator.getNumInputs(); i++) {
            operatorContext.setInputCardinality(i, new CardinalityEstimate(100, 10000, 0.1));
        }
        for (int i = 0; i < operator.getNumOutputs(); i++) {
            operatorContext.setOutputCardinality(i, new CardinalityEstimate(100, 10000, 0.1));
        }
        return operatorContext;
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
