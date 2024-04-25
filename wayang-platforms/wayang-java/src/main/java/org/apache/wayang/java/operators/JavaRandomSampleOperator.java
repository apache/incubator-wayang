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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link JavaRandomSampleOperator}. This sampling method is with replacement (i.e., duplicates may appear in the sample).
 */
public class JavaRandomSampleOperator<Type>
        extends SampleOperator<Type>
        implements JavaExecutionOperator {

    private Random rand;

    /**
     * Creates a new instance.
     *
     * @param sampleSizeFunction udf-based size of sample
     */
    public JavaRandomSampleOperator(FunctionDescriptor.SerializableIntUnaryOperator sampleSizeFunction, DataSetType<Type> type, FunctionDescriptor.SerializableLongUnaryOperator seedFunction) {
        super(sampleSizeFunction, type, Methods.RANDOM, seedFunction);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaRandomSampleOperator(SampleOperator<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();


        Integer sampleSize = (Integer) this.getSampleSize(operatorContext);
        Long datasetSize = this.isDataSetSizeKnown() ? this.getDatasetSize() :
                ((CollectionChannel.Instance) inputs[0]).provideCollection().size();

        if (sampleSize >= datasetSize) { //return all
            ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).provideStream());
        }
        else {
            long seed = this.getSeed(operatorContext);
            rand = new Random(seed);

            final int[] sampleIndices = new int[sampleSize];
            final BitSet data = new BitSet();
            for (int i = 0; i < sampleSize; i++) {
                sampleIndices[i] = rand.nextInt(datasetSize.intValue());
                while (data.get(sampleIndices[i])) //without replacement
                    sampleIndices[i] = rand.nextInt(datasetSize.intValue());
                data.set(sampleIndices[i]);
            }
            Arrays.sort(sampleIndices);

            ((StreamChannel.Instance) outputs[0]).accept(((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(new Predicate<Type>() {
                        int streamIndex = 0;
                        int sampleIndex = 0;

                @Override
                        public boolean test(Type element) {
                            if (sampleIndex == sampleIndices.length) //we already picked all our samples
                                return false;
                            if (streamIndex == sampleIndices[sampleIndex]) {
                                sampleIndex++;
                                streamIndex++;
                                return true;
                            }
                            streamIndex++;
                            return false;
                        }
                    })
            );
        }

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Collections.singletonList("wayang.java.random-sample.load");
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaRandomSampleOperator<>(this);
    }


    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return this.isDataSetSizeKnown() ?
                Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR) :
                Collections.singletonList(CollectionChannel.DESCRIPTOR);

    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
