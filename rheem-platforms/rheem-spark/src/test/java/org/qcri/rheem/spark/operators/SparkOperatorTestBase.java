package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.BeforeClass;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by yidris on 2/6/16.
 */
public class SparkOperatorTestBase {

    protected SparkExecutor sparkExecutor;

    @Before
    public void setUp() {
        this.sparkExecutor = (SparkExecutor) SparkPlatform.getInstance().getExecutorFactory().create(mockJob());
    }

    protected Job mockJob() {
        final Job job = mock(Job.class);
        when(job.getConfiguration()).thenReturn(new Configuration());
        return job;
    }



    public JavaSparkContext getSC() {
        return this.sparkExecutor.sc;
    }

}
