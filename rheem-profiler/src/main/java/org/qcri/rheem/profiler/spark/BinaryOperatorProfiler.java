package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.spark.channels.ChannelExecutor;
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
        final ChannelExecutor inputChannelExecutor0 = createChannelExecutor(this.inputRdd0, this.sparkExecutor);
        final ChannelExecutor inputChannelExecutor1 = createChannelExecutor(this.inputRdd1, this.sparkExecutor);
        final ChannelExecutor outputChannelExecutor = createChannelExecutor(this.sparkExecutor);

        // Let the operator execute.
        ProfilingUtils.sleep(this.executionPaddingTime); // Pad measurement with some idle time.
        final long startTime = System.currentTimeMillis();
        this.operator.evaluate(
                new ChannelExecutor[] { inputChannelExecutor0, inputChannelExecutor1 },
                new ChannelExecutor[] { outputChannelExecutor },
                this.functionCompiler,
                this.sparkExecutor
        );

        // Force the execution of the operator.
        outputChannelExecutor.provideRdd().foreach(dataQuantum -> { });
        final long endTime = System.currentTimeMillis();
        ProfilingUtils.sleep(this.executionPaddingTime); // Pad measurement with some idle time.

        // Yet another run to count the output cardinality.
        final long outputCardinality = outputChannelExecutor.provideRdd().count();

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
