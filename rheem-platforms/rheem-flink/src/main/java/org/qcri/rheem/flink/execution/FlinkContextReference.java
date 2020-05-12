package org.qcri.rheem.flink.execution;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.qcri.rheem.core.api.Configuration;
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
    public FlinkContextReference(CrossPlatformExecutor crossPlatformExecutor, ExecutionEnvironment flinkEnviroment, int parallelism) {
        super(null);
        if (crossPlatformExecutor != null) {
            crossPlatformExecutor.registerGlobal(this);
        }
        this.flinkEnviroment = flinkEnviroment;
        loadConfiguration( crossPlatformExecutor.getConfiguration(), parallelism );
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

    private void loadConfiguration(Configuration conf, int parallelism){
        ParameterTool tools = ParameterTool.fromSystemProperties();
        this.flinkEnviroment.getConfig().setGlobalJobParameters(tools);
        this.flinkEnviroment.setParallelism(parallelism);

        ExecutionMode mode = getExecutionMode(conf.getStringProperty("rheem.flink.mode.execution"));

        this.flinkEnviroment.getConfig().setExecutionMode(mode);
    }

    private ExecutionMode getExecutionMode(String name){
        ExecutionMode mode;
        switch (name){
            case "batch_forced":
                mode = ExecutionMode.BATCH_FORCED;
                break;
            case "batch":
                mode = ExecutionMode.BATCH;
                break;
            case "pipelined":
                mode = ExecutionMode.PIPELINED;
                break;
            case "pipelined_forced":
                mode = ExecutionMode.PIPELINED_FORCED;
                break;
            default:
                mode = ExecutionMode.BATCH_FORCED;
                break;
        }
        return mode;
    }
}
