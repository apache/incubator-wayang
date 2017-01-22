package org.qcri.rheem.jdbc.operators;

import org.junit.BeforeClass;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.optimizer.DefaultOptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.profiling.FullInstrumentationStrategy;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.platform.JavaPlatform;

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
