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

package org.apache.wayang.flink.execution;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.Executor;
import org.apache.wayang.core.platform.PartialExecution;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.platform.PushExecutorTemplate;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.compiler.FunctionCompiler;
import org.apache.wayang.flink.operators.FlinkExecutionOperator;
import org.apache.wayang.flink.platform.FlinkPlatform;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link Executor} implementation for the {@link FlinkPlatform}.
 */
public class FlinkExecutor extends PushExecutorTemplate {
    /**
     * Reference to a {@link ExecutionEnvironment} to be used by this instance.
     */
    private FlinkContextReference flinkContextReference;

    /**
     * The {@link ExecutionEnvironment} to be used by this instance.
     *
     */
    public ExecutionEnvironment fee;

    /**
     * Compiler to create flink UDFs.
     */
    public FunctionCompiler compiler = new FunctionCompiler();

    /**
     * Reference to the {@link ExecutionEnvironment} that provides the FlinkContextReference.
     */
    private FlinkPlatform platform;

    /**
     * The requested number of partitions. Should be incorporated by {@link FlinkExecutionOperator}s.
     */
    private int numDefaultPartitions;


    public FlinkExecutor(FlinkPlatform flinkPlatform, Job job) {
        super(job);
        this.platform = flinkPlatform;
        this.flinkContextReference = this.platform.getFlinkContext(job);
        this.fee = this.flinkContextReference.get();
        this.numDefaultPartitions = (int) this.getConfiguration().getLongProperty("wayang.flink.parallelism");
        this.fee.setParallelism(this.numDefaultPartitions);
        this.flinkContextReference.noteObtainedReference();
    }

    @Override
    protected Tuple<List<ChannelInstance>, PartialExecution> execute(
                                            ExecutionTask task,
                                            List<ChannelInstance> inputChannelInstances,
                                            OptimizationContext.OperatorContext producerOperatorContext,
                                            boolean isRequestEagerExecution) {
        // Provide the ChannelInstances for the output of the task.
        final ChannelInstance[] outputChannelInstances = task.getOperator().createOutputChannelInstances(
                this, task, producerOperatorContext, inputChannelInstances
        );

        // Execute.
        final Collection<ExecutionLineageNode> executionLineageNodes;
        final Collection<ChannelInstance> producedChannelInstances;
        // TODO: Use proper progress estimator.
        this.job.reportProgress(task.getOperator().getName(), 50);

        long startTime = System.currentTimeMillis();
        try {
            final Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> results =
                    cast(task.getOperator()).evaluate(
                            toArray(inputChannelInstances),
                            outputChannelInstances,
                            this,
                            producerOperatorContext
                    );
            executionLineageNodes = results.getField0();
            producedChannelInstances = results.getField1();
        } catch (Exception e) {
            throw new WayangException(String.format("Executing %s failed.", task), e);
        }
        long endTime = System.currentTimeMillis();
        long executionDuration = endTime - startTime;
        this.job.reportProgress(task.getOperator().getName(), 100);

        // Check how much we executed.
        PartialExecution partialExecution = this.createPartialExecution(executionLineageNodes, executionDuration);

        if (partialExecution == null && executionDuration > 10) {
            this.logger.warn("Execution of {} took suspiciously long ({}).", task, Formats.formatDuration(executionDuration));
        }

        // Collect any cardinality updates.
        this.registerMeasuredCardinalities(producedChannelInstances);

        // Warn if requested eager execution did not take place.
        if (isRequestEagerExecution ){
            if( partialExecution == null) {
                this.logger.info("{} was not executed eagerly as requested.", task);
            }else {
                try {
                    //TODO validate the execute in different contexts
                    this.fee.execute();
                } catch (Exception e) {
                    throw new WayangException(e);
                }
            }
        }
        return new Tuple<>(Arrays.asList(outputChannelInstances), partialExecution);
    }

    @Override
    public void dispose() {
        super.dispose();
        this.flinkContextReference.noteDiscardedReference(false);
    }

    @Override
    public Platform getPlatform() {
        return this.platform;
    }

    private static FlinkExecutionOperator cast(ExecutionOperator executionOperator) {
        return (FlinkExecutionOperator) executionOperator;
    }

    private static ChannelInstance[] toArray(List<ChannelInstance> channelInstances) {
        final ChannelInstance[] array = new ChannelInstance[channelInstances.size()];
        return channelInstances.toArray(array);
    }

    /**
     * Provide a {@link FunctionCompiler}.
     *
     * @return the {@link FunctionCompiler}
     */
    public FunctionCompiler getCompiler() {
        return this.compiler;
    }

    public int getNumDefaultPartitions(){
        return this.numDefaultPartitions;
    }
}
