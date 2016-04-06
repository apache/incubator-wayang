package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.spark.channels.ChannelExecutor;
import org.qcri.rheem.spark.operators.SparkExecutionOperator;

import java.util.function.Supplier;

/**
 * {@link SparkOperatorProfiler} implementation for {@link SparkExecutionOperator}s with one input and one output.
 */
public class UnaryOperatorProfiler extends SparkOperatorProfiler {

    private JavaRDD<?> inputRdd;

    public UnaryOperatorProfiler(Supplier<SparkExecutionOperator> operatorGenerator,
                                 Configuration configuration,
                                 Supplier<?> dataQuantumGenerator) {
        super(operatorGenerator, configuration, dataQuantumGenerator);
    }

    @Override
    protected void prepareInput(int inputIndex, long inputCardinality) {
        assert inputIndex == 0;
        this.inputRdd = this.prepareInputRdd(inputCardinality, inputIndex);
    }

    @Override
    protected Result executeOperator() {
        final ChannelExecutor inputChannelExecutor = createChannelExecutor(this.inputRdd, this.sparkExecutor);
        final ChannelExecutor outputChannelExecutor = createChannelExecutor(this.sparkExecutor);

        // Let the operator execute.
        final long startTime = System.currentTimeMillis();
        this.operator.evaluate(
                new ChannelExecutor[] { inputChannelExecutor },
                new ChannelExecutor[] { outputChannelExecutor },
                this.functionCompiler,
                this.sparkExecutor
        );

        // Force the execution of the operator.
        outputChannelExecutor.provideRdd().foreach(dataQuantum -> { });
        final long endTime = System.currentTimeMillis();

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
