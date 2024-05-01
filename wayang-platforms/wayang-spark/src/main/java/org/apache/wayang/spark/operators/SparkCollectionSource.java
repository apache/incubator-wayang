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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Provides a {@link Collection} to a Spark job. Can also be used to convert {@link CollectionChannel}s of the
 * {@link JavaPlatform} into {@link RddChannel}s.
 */
public class SparkCollectionSource<Type> extends CollectionSource<Type> implements SparkExecutionOperator {

    /**
     * Create a new instance to convert a {@link CollectionChannel} into a {@link RddChannel}.
     */
    public SparkCollectionSource(DataSetType<Type> type) {
        this(null, type);
    }

    /**
     * Create a new instance to use a {@code collection} in a {@link WayangPlan}.
     */
    public SparkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkCollectionSource(CollectionSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length <= 1;
        assert outputs.length == this.getNumOutputs();

        final Collection<Type> collection;
        if (this.collection != null) {
            collection = this.collection;
        } else {
            final CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
            collection = input.provideCollection();
            assert collection != null : String.format("Instance of %s is not providing a collection.", input.getChannel());
        }
        final List<Type> list = WayangCollections.asList(collection);

        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];
        final JavaRDD<Type> rdd = sparkExecutor.sc.parallelize(list, sparkExecutor.getNumDefaultPartitions());
        this.name(rdd);
        output.accept(rdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.collectionsource.load";
    }

    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override public boolean isConversion() {
        return true;
    }
}
