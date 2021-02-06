/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.profiler.util.ProfilingUtils;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

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
