package org.qcri.rheem.flink.execution;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.qcri.rheem.core.platform.CrossPlatformExecutor;
import org.qcri.rheem.core.platform.ExecutionResourceTemplate;

/**
 * Wraps and manages a Flink {@link ExecutionEnvironment} to avoid steady re-creation.
 */
public class FlinkContextReference extends ExecutionResourceTemplate {


    /**
     * The wrapped {@link ExecutionEnvironment}.
     */
    private ExecutionEnvironment flinkEnviroment;

    /**
     * Creates a new instance.
     *
     * @param flinkEnviroment the {@link ExecutionEnvironment} to be wrapped
     */
    public FlinkContextReference(CrossPlatformExecutor crossPlatformExecutor, ExecutionEnvironment flinkEnviroment) {
        super(null);
        if (crossPlatformExecutor != null) {
            crossPlatformExecutor.registerGlobal(this);
        }
        this.flinkEnviroment = flinkEnviroment;
    }


    /**
     * Provides the {@link ExecutionEnvironment}. This instance must not be disposed, yet.
     *
     * @return the wrapped {@link ExecutionEnvironment}
     */
    public ExecutionEnvironment get() {

        return this.flinkEnviroment;
    }

    @Override
    protected void doDispose() throws Throwable {

    }

    @Override
    public boolean isDisposed() {
        return false;
    }
}
