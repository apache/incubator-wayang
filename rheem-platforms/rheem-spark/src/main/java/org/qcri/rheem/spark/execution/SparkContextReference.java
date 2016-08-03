package org.qcri.rheem.spark.execution;

import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionResourceTemplate;

/**
 * Wraps and manages a {@link JavaSparkContext} to avoid steady re-creation.
 */
public class SparkContextReference extends ExecutionResourceTemplate {

    /**
     * The wrapped {@link JavaSparkContext}.
     */
    private final JavaSparkContext sparkContext;

    /**
     * Creates a new instance.
     *
     * @param sparkContext the {@link JavaSparkContext} to be wrapped
     */
    public SparkContextReference(CrossPlatformExecutor crossPlatformExecutor, JavaSparkContext sparkContext) {
        super(null);
        if (crossPlatformExecutor != null) {
            crossPlatformExecutor.registerGlobal(this);
        }
        this.sparkContext = sparkContext;
    }

    @Override
    protected void doDispose() throws Throwable {
        assert !this.isDisposed();

        this.sparkContext.close();
    }

    /**
     * Provides the {@link JavaSparkContext}. This instance must not be disposed, yet.
     *
     * @return the wrapped {@link JavaSparkContext}
     */
    public JavaSparkContext get() {
        assert !this.isDisposed();
        return this.sparkContext;
    }
}
