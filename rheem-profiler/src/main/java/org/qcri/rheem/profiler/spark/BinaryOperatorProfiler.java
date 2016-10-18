package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.spark.channels.RddChannel;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with two inputs and one output.
 */
public class BinaryOperatorProfiler extends SparkOperatorProfiler {

    private JavaRDD<?> inputRdd0, inputRdd1;

    public BinaryOperatorProfiler(Supplier<SparkExecutionOperator> operatorGenerator,
                                  Configuration configuration,
                                  Supplier<?> dataQuantumGenerator0,
                                  Supplier<?> dataQuantumGenerator1) {
        super(operatorGenerator, configuration, dataQuantumGenerator0, dataQuantumGenerator1);
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        switch (inputIndex) {
            case 0:
                this.inputRdd0 = this.prepareInputRdd(inputCardinality, inputIndex);
                break;
            case 1:
                this.inputRdd1 = this.prepareInputRdd(inputCardinality, inputIndex);
                break;
            default:
                assert false;
        }
    }

    @Override
    protected Result executeOperator() {
        final RddChannel.Instance inputChannelInstance0 = createChannelInstance(this.inputRdd0, this.sparkExecutor);
        final RddChannel.Instance inputChannelInstance1 = createChannelInstance(this.inputRdd1, this.sparkExecutor);
        final RddChannel.Instance outputChannelInstance = createChannelInstance(this.sparkExecutor);

        // Let the operator execute.
        ProfilingUtils.sleep(this.executionPaddingTime); // Pad measurement with some idle time.
        final long startTime = System.currentTimeMillis();
        this.evaluate(
                this.operator,
                new ChannelInstance[]{inputChannelInstance0, inputChannelInstance1},
                new ChannelInstance[]{outputChannelInstance}
        );

        // Force the execution of the operator.
        outputChannelInstance.provideRdd().foreach(dataQuantum -> {
        });
        final long endTime = System.currentTimeMillis();
        ProfilingUtils.sleep(this.executionPaddingTime); // Pad measurement with some idle time.

        // Yet another run to count the output cardinality.
        final long outputCardinality = outputChannelInstance.provideRdd().count();

        // Gather and assemble all result metrics.
        return new Result(
                this.inputCardinalities,
                outputCardinality,
                endTime - startTime,
                this.provideDiskBytes(startTime, endTime),
                this.provideNetworkBytes(startTime, endTime),
                this.provideCpuCycles(startTime, endTime),
                this.numMachines,
                this.numCoresPerMachine
        );
    }
}
