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

package org.apache.wayang.flink.operators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import io.altoo.akka.serialization.kryo.KryoSerializer;
import org.apache.flink.util.SplittableIterator;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.flink.channels.DataSetChannel;
import org.apache.wayang.flink.execution.FlinkExecutor;
import org.apache.wayang.java.channels.CollectionChannel;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.Collections;

/**
 * This is execution operator implements the {@link CollectionSource}.
 */
public class FlinkCollectionSource<Type> extends CollectionSource<Type> implements FlinkExecutionOperator{
    public FlinkCollectionSource(DataSetType<Type> type) {
        this(null, type);
    }

    public FlinkCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public FlinkCollectionSource(CollectionSource<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            FlinkExecutor flinkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        //assert inputs.length == 0;
        //assert outputs.length == 1;

        final Collection<Type> collection;
        if (this.collection != null) {
            collection = this.collection;
        } else {
            collection = ((CollectionChannel.Instance)inputs[0]).provideCollection();
        }

        SplittableIterator<Type> iterator = new CollectionSplittableIterator<Type>(new ArrayList(collection), flinkExecutor.fee.getParallelism());
        if (collection.iterator().hasNext()) {
            Type firstValue = collection.iterator().next();

            TypeInformation<Type> type = TypeExtractor.getForObject(firstValue);

            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(ExecutionLineageNode.class, DefaultSerializers.ClassSerializer.class);
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(ChannelInstance.class, DefaultSerializers.ClassSerializer.class);
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(CollectionSplittableIterator.class, DefaultSerializers.ClassSerializer.class);
            /*
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(scala.Tuple5.class, Tuple5Serializer.class);
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(scala.Tuple4.class, Tuple4Serializer.class);
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(scala.Tuple3.class, Tuple3Serializer.class);
            flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(scala.Tuple2.class, Tuple2Serializer.class);
            */

            System.out.println("Got type: " + firstValue.getClass());
            if (firstValue.getClass().getName().contains("scala.Tuple")) {
                flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(firstValue.getClass(), ScalaTupleSerializer.class);
            }
            //flinkExecutor.fee.getConfig().registerPojoType(firstValue.getClass());
            //flinkExecutor.fee.getConfig().registerTypeWithKryoSerializer(firstValue.getClass(), new KryoSerializer(firstValue.getClass(), flinkExecutor.fee.getConfig()));



            final DataSet<Type> datasetOutput = flinkExecutor.fee.fromCollection(collection.parallelStream().collect(Collectors.toList()))
                .setParallelism(flinkExecutor.fee.getParallelism());
            /*
            final DataSet<Type> datasetOutput = flinkExecutor.fee.fromParallelCollection(iterator, type)
                .setParallelism(flinkExecutor.fee.getParallelism());*/

            ((DataSetChannel.Instance) outputs[0]).accept(datasetOutput, flinkExecutor);

        } else {
            //Extremely hacky for empty lists - lord forgive me
            //This needs to be done as .fromCollection throws exceptions
            //on an empty list, but in Wayang we can operate on empty results.
            final DataSet<int[]> datasetOutput = flinkExecutor.fee
                .fromCollection(List.of(new int[]{0}))
                .setParallelism(flinkExecutor.fee.getParallelism());

            ((DataSetChannel.Instance) outputs[0]).accept(
                datasetOutput.filter(x -> false)
                ,flinkExecutor);
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.flink.collectionsource.load";
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Arrays.asList(DataSetChannel.DESCRIPTOR, DataSetChannel.DESCRIPTOR_MANY);
    }

    @Override public boolean isConversion() {
        return true;
    }

}
