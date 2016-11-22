package org.qcri.rheem.java.operators;

import org.junit.BeforeClass;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.profiling.NoInstrumentationStrategy;
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
