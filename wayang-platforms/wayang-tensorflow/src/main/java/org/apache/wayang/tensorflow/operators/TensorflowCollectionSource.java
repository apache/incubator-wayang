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

package org.apache.wayang.tensorflow.operators;

import org.apache.wayang.basic.operators.CollectionSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.tensorflow.channels.TensorChannel;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;
import org.tensorflow.ndarray.NdArray;
import org.tensorflow.ndarray.StdArrays;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Provides a {@link Collection} to a Tensorflow job. Can also be used to convert {@link CollectionChannel}s of the
 * {@link JavaPlatform} into {@link TensorChannel}s.
 */
public class TensorflowCollectionSource<Type> extends CollectionSource<Type> implements TensorflowExecutionOperator {

    public TensorflowCollectionSource(DataSetType<Type> type) {
        this(null, type);
    }

    public TensorflowCollectionSource(Collection<Type> collection, DataSetType<Type> type) {
        super(collection, type);
    }

    public TensorflowCollectionSource(CollectionSource that) {
        super(that);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new TensorflowCollectionSource<>(this.getCollection(), this.getType());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(TensorChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            TensorflowExecutor tensorflowExecutor,
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
        assert collection.size() > 0 : "Collection is empty";
        final List<Type> list = WayangCollections.asList(collection);

        final NdArray<?> ndArray;
        final Type e = list.get(0);

        if (e instanceof Integer) {
            ndArray = StdArrays.ndCopyOf(list.stream().mapToInt(o -> (int) o).toArray());
        } else if (e instanceof int[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new int[0][]));
        } else if (e instanceof int[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new int[0][][]));
        } else if (e instanceof int[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new int[0][][][]));
        } else if (e instanceof int[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new int[0][][][][]));
        } else if (e instanceof int[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new int[0][][][][][]));
        }

        else if (e instanceof Long) {
            ndArray = StdArrays.ndCopyOf(list.stream().mapToLong(o -> (long) o).toArray());
        } else if (e instanceof long[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new long[0][]));
        } else if (e instanceof long[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new long[0][][]));
        } else if (e instanceof long[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new long[0][][][]));
        } else if (e instanceof long[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new long[0][][][][]));
        } else if (e instanceof long[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new long[0][][][][][]));
        }

        else if (e instanceof Float) {
            float[] floats = new float[list.size()];
            for (int i = 0; i < list.size(); i++) {
                floats[i] = (float) list.get(i);
            }
            ndArray = StdArrays.ndCopyOf(floats);
        } else if (e instanceof float[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new float[0][]));
        } else if (e instanceof float[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new float[0][][]));
        } else if (e instanceof float[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new float[0][][][]));
        } else if (e instanceof float[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new float[0][][][][]));
        } else if (e instanceof float[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new float[0][][][][][]));
        }

        else if (e instanceof Double) {
            ndArray = StdArrays.ndCopyOf(list.stream().mapToDouble(o -> (double) o).toArray());
        } else if (e instanceof double[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new double[0][]));
        } else if (e instanceof double[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new double[0][][]));
        } else if (e instanceof double[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new double[0][][][]));
        } else if (e instanceof double[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new double[0][][][][]));
        } else if (e instanceof double[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new double[0][][][][][]));
        }

        else if (e instanceof Byte) {
            byte[] bytes = new byte[list.size()];
            for (int i = 0; i < list.size(); i++) {
                bytes[i] = (byte) list.get(i);
            }
            ndArray = StdArrays.ndCopyOf(bytes);
        } else if (e instanceof byte[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new byte[0][]));
        } else if (e instanceof byte[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new byte[0][][]));
        } else if (e instanceof byte[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new byte[0][][][]));
        } else if (e instanceof byte[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new byte[0][][][][]));
        } else if (e instanceof byte[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new byte[0][][][][][]));
        }

        else if (e instanceof Short) {
            short[] shorts = new short[list.size()];
            for (int i = 0; i < list.size(); i++) {
                shorts[i] = (short) list.get(i);
            }
            ndArray = StdArrays.ndCopyOf(shorts);
        } else if (e instanceof short[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new short[0][]));
        } else if (e instanceof short[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new short[0][][]));
        } else if (e instanceof short[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new short[0][][][]));
        } else if (e instanceof short[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new short[0][][][][]));
        } else if (e instanceof short[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new short[0][][][][][]));
        }

        else if (e instanceof Boolean) {
            boolean[] booleans = new boolean[list.size()];
            for (int i = 0; i < list.size(); i++) {
                booleans[i] = (boolean) list.get(i);
            }
            ndArray = StdArrays.ndCopyOf(booleans);
        } else if (e instanceof boolean[]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new boolean[0][]));
        } else if (e instanceof boolean[][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new boolean[0][][]));
        } else if (e instanceof boolean[][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new boolean[0][][][]));
        } else if (e instanceof boolean[][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new boolean[0][][][][]));
        } else if (e instanceof boolean[][][][][]) {
            ndArray = StdArrays.ndCopyOf(list.toArray(new boolean[0][][][][][]));
        } else if (e instanceof String) {
            ndArray = StdArrays.ndCopyOf(list.stream().toArray());
        }else {
            throw new RuntimeException("Unsupported element type: " + e.getClass().getName() + "; expected: " + this.getType());
        }

        final TensorChannel.Instance output = (TensorChannel.Instance) outputs[0];
        output.accept(ndArray);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);

    }
}
