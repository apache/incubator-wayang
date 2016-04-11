package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and one output.
 */
public abstract class SparkSourceProfiler extends SparkOperatorProfiler {

    public SparkSourceProfiler(Supplier<SparkExecutionOperator> operatorGenerator,
                               Configuration configuration,
                               Supplier<?> dataQuantumGenerator) {
        super(operatorGenerator, configuration, dataQuantumGenerator);
    }

    @Override
    protected Result executeOperator() {
        final ChannelExecutor outputChannelExecutor = createChannelExecutor(this.sparkExecutor);

        // Let the operator execute.
        ProfilingUtils.sleep(this.executionPaddingTime); // Pad measurement with some idle time.
        final long startTime = System.currentTimeMillis();
        this.operator.evaluate(
                new ChannelExecutor[]{},
                new ChannelExecutor[]{outputChannelExecutor},
                this.functionCompiler,
                this.sparkExecutor
        );

        // Force the execution of the operator.
        outputChannelExecutor.provideRdd().foreach(dataQuantum -> {
        });
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
