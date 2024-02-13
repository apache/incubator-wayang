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

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.CoGroupOperator;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.compiler.FunctionCompiler;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Spark implementation of the {@link JoinOperator}.
 */
public class SparkCoGroupOperator<In0, In1, Key> extends CoGroupOperator<In0, In1, Key> implements SparkExecutionOperator {

    /**
     * @see CoGroupOperator#CoGroupOperator(FunctionDescriptor.SerializableFunction, FunctionDescriptor.SerializableFunction, Class, Class, Class)
     */
    public SparkCoGroupOperator(FunctionDescriptor.SerializableFunction<In0, Key> keyExtractor0,
                                FunctionDescriptor.SerializableFunction<In1, Key> keyExtractor1,
                                Class<In0> input0Class,
                                Class<In1> input1Class,
                                Class<Key> keyClass) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, keyClass);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor, TransformationDescriptor)
     */
    public SparkCoGroupOperator(TransformationDescriptor<In0, Key> keyDescriptor0,
                                TransformationDescriptor<In1, Key> keyDescriptor1) {
        super(keyDescriptor0, keyDescriptor1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(TransformationDescriptor, TransformationDescriptor, DataSetType, DataSetType)
     */
    public SparkCoGroupOperator(TransformationDescriptor<In0, Key> keyDescriptor0,
                                TransformationDescriptor<In1, Key> keyDescriptor1,
                                DataSetType<In0> inputType0,
                                DataSetType<In1> inputType1) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1);
    }

    /**
     * @see CoGroupOperator#CoGroupOperator(CoGroupOperator)
     */
    public SparkCoGroupOperator(CoGroupOperator<In0, In1, Key> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final RddChannel.Instance input0 = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance input1 = (RddChannel.Instance) inputs[1];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<In0> inputRdd0 = input0.provideRdd();
        final JavaRDD<In1> inputRdd1 = input1.provideRdd();

        FunctionCompiler compiler = sparkExecutor.getCompiler();
        final PairFunction<In0, Key, In0> keyExtractor0 = compiler.compileToKeyExtractor(this.keyDescriptor0);
        final PairFunction<In1, Key, In1> keyExtractor1 = compiler.compileToKeyExtractor(this.keyDescriptor1);
        JavaPairRDD<Key, In0> pairRdd0 = inputRdd0.mapToPair(keyExtractor0);
        JavaPairRDD<Key, In1> pairRdd1 = inputRdd1.mapToPair(keyExtractor1);

        final JavaPairRDD<Key, scala.Tuple2<Iterable<In0>, Iterable<In1>>> outputPair =
                pairRdd0.cogroup(pairRdd1, sparkExecutor.getNumDefaultPartitions());
        this.name(outputPair);

        // Map the output to what Wayang expects.
        final JavaRDD<Tuple2<Iterable<In0>, Iterable<In1>>> outputRdd = outputPair.map(new TupleConverter<>());
        this.name(outputRdd);

        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkCoGroupOperator<>(this);
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.cogroup.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
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

    /**
     * Converts the output of {@link JavaPairRDD#cogroup(JavaPairRDD, int)} to what Wayang expects.
     * <p><i>TODO: See, if we can somehow dodge all this conversion, which is likely to happen a lot.</i></p>
     */
    private static class TupleConverter<InputType0, InputType1, KeyType>
            implements Function<scala.Tuple2<KeyType, scala.Tuple2<Iterable<InputType0>, Iterable<InputType1>>>, Tuple2<Iterable<InputType0>, Iterable<InputType1>>> {

        @Override
        public Tuple2<Iterable<InputType0>, Iterable<InputType1>> call(scala.Tuple2<KeyType, scala.Tuple2<Iterable<InputType0>, Iterable<InputType1>>> in) throws Exception {
            return new Tuple2<>(in._2._1, in._2._2);
        }
    }
}
