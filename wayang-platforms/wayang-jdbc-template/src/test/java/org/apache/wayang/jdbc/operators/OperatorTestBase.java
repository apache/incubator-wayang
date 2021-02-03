package org.apache.wayang.jdbc.operators;

import org.junit.BeforeClass;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.CrossPlatformExecutor;
import org.apache.wayang.core.profiling.FullInstrumentationStrategy;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.java.platform.JavaPlatform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test base for {@link JdbcExecutionOperator}s and other {@link ExecutionOperator}s in this module.
 */
public class OperatorTestBase {

    protected static Configuration configuration;

    @BeforeClass
    public static void init() {
        configuration = new Configuration();
    }

    protected static JavaExecutor createJavaExecutor() {
        final Job job = createJob();
        return new JavaExecutor(JavaPlatform.getInstance(), job);
    }

    private static Job createJob() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(configuration);
        when(job.getCrossPlatformExecutor()).thenReturn(new CrossPlatformExecutor(job, new FullInstrumentationStrategy()));
        return job;
    }

    protected static OptimizationContext.OperatorContext createOperatorContext(Operator operator) {
        OptimizationContext optimizationContext = new DefaultOptimizationContext(createJob());
        return optimizationContext.addOneTimeOperator(operator);
    }

    protected static void evaluate(JavaExecutionOperator operator,
                                   ChannelInstance[] inputs,
                                   ChannelInstance[] outputs) {
        operator.evaluate(inputs, outputs, createJavaExecutor(), createOperatorContext(operator));
    }

}
