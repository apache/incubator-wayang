package org.qcri.rheem.spark.operators;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.spark.platform.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;

/**
 * Created by yidris on 2/6/16.
 */
public class SparkOperatorTestBase {

    protected final SparkExecutor sparkExecutor = (SparkExecutor) SparkPlatform
            .getInstance().getExecutorFactory().create(Configuration.getDefaultConfiguration());

    public JavaSparkContext getSC() {
        return this.sparkExecutor.sc;
    }

}
