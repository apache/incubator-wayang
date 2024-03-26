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

import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.tensorflow.channels.TensorChannel;
import org.apache.wayang.tensorflow.execution.TensorflowExecutor;
import org.tensorflow.ndarray.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Converts {@link TensorChannel} into a {@link CollectionChannel}
 */
public class TensorflowCollectOperator<Type> extends UnaryToUnaryOperator<NdArray, Type> implements TensorflowExecutionOperator {

    public TensorflowCollectOperator(DataSetType<Type> type) {
        super(DataSetType.createDefaultUnchecked(NdArray.class), type, false);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(TensorChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            TensorflowExecutor tensorflowExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final TensorChannel.Instance input = (TensorChannel.Instance) inputs[0];
        final CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        NdArray ndArray = input.provideTensor();
        final long[] shape = ndArray.shape().asArray();

        final List<Type> list;
        if (shape.length == 1) {
            if (ndArray instanceof IntNdArray) {
                int[] dst = new int[(int) shape[0]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).boxed().collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[] dst = new long[(int) shape[0]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).boxed().collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[] dst = new float[(int) shape[0]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = new ArrayList<>((int) shape[0]);
                for (float v : dst) {
                    list.add((Type) (Float) v);
                }
            } else if (ndArray instanceof DoubleNdArray) {
                double[] dst = new double[(int) shape[0]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).boxed().collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[] dst = new short[(int) shape[0]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = new ArrayList<>((int) shape[0]);
                for (short value : dst) {
                    list.add((Type) (Short) value);
                }
            } else if (ndArray instanceof ByteNdArray) {
                byte[] dst = new byte[(int) shape[0]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = new ArrayList<>((int) shape[0]);
                for (byte b : dst) {
                    list.add((Type) (Byte) b);
                }
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[] dst = new boolean[(int) shape[0]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = new ArrayList<>((int) shape[0]);
                for (boolean b : dst) {
                    list.add((Type) (Boolean) b);
                }
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else if (shape.length == 2) {
            if (ndArray instanceof IntNdArray) {
                int[][] dst = new int[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[][] dst = new long[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[][] dst = new float[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof DoubleNdArray) {
                double[][] dst = new double[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[][] dst = new short[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ByteNdArray) {
                byte[][] dst = new byte[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[][] dst = new boolean[(int) shape[0]][(int) shape[1]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else if (shape.length == 3) {
            if (ndArray instanceof IntNdArray) {
                int[][][] dst = new int[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[][][] dst = new long[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[][][] dst = new float[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof DoubleNdArray) {
                double[][][] dst = new double[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[][][] dst = new short[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ByteNdArray) {
                byte[][][] dst = new byte[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[][][] dst = new boolean[(int) shape[0]][(int) shape[1]][(int) shape[2]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else if (shape.length == 4) {
            if (ndArray instanceof IntNdArray) {
                int[][][][] dst = new int[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[][][][] dst = new long[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[][][][] dst = new float[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof DoubleNdArray) {
                double[][][][] dst = new double[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[][][][] dst = new short[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ByteNdArray) {
                byte[][][][] dst = new byte[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[][][][] dst = new boolean[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else if (shape.length == 5) {
            if (ndArray instanceof IntNdArray) {
                int[][][][][] dst = new int[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[][][][][] dst = new long[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[][][][][] dst = new float[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof DoubleNdArray) {
                double[][][][][] dst = new double[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[][][][][] dst = new short[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ByteNdArray) {
                byte[][][][][] dst = new byte[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[][][][][] dst = new boolean[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else if (shape.length == 6) {
            if (ndArray instanceof IntNdArray) {
                int[][][][][][] dst = new int[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((IntNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof LongNdArray) {
                long[][][][][][] dst = new long[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((LongNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof FloatNdArray) {
                float[][][][][][] dst = new float[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((FloatNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof DoubleNdArray) {
                double[][][][][][] dst = new double[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((DoubleNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ShortNdArray) {
                short[][][][][][] dst = new short[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((ShortNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof ByteNdArray) {
                byte[][][][][][] dst = new byte[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((ByteNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else if (ndArray instanceof BooleanNdArray) {
                boolean[][][][][][] dst = new boolean[(int) shape[0]][(int) shape[1]][(int) shape[2]][(int) shape[3]][(int) shape[4]][(int) shape[5]];
                StdArrays.copyFrom((BooleanNdArray) ndArray, dst);
                list = (List<Type>) Arrays.stream(dst).collect(Collectors.toList());
            } else {
                throw new RuntimeException("Unsupported NdArray type.");
            }
        }

        else {
            throw new RuntimeException(String.format("Unsupported shape: %s.", Arrays.toString(shape)));
        }

        output.accept(list);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }
}
