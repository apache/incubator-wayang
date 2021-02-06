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

package org.apache.wayang.iejoin.operators;

import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.iejoin.data.Data;
import org.apache.wayang.iejoin.operators.java_helpers.BitSetJoin;
import org.apache.wayang.iejoin.operators.java_helpers.DataComparator;
import org.apache.wayang.iejoin.operators.java_helpers.extractData;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Java implementation of the {@link IESelfJoinOperator}.
 */
public class JavaIESelfJoinOperator<Type0 extends Comparable<Type0>, Type1 extends Comparable<Type1>, Input>
        extends IESelfJoinOperator<Type0, Type1, Input>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     */
    public JavaIESelfJoinOperator(DataSetType<Input> inputType0,
                                  TransformationDescriptor<Input, Type0> get0Pivot, IEJoinMasterOperator.JoinCondition cond0,
                                  TransformationDescriptor<Input, Type1> get0Ref, IEJoinMasterOperator.JoinCondition cond1) {
        super(inputType0, get0Pivot, cond0, get0Ref, cond1);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        StreamChannel.Instance outputChannel = (StreamChannel.Instance) outputs[0];

        Stream<Input> stream0;
        if (inputs[0] instanceof CollectionChannel.Instance) {
            final Collection<Input> collection = ((CollectionChannel.Instance) inputs[0]).provideCollection();
            stream0 = collection.stream();
        } else {
            // Fallback: Materialize one side.
            final Collection<Input> collection = ((JavaChannelInstance) inputs[0]).<Input>provideStream().collect(Collectors.toList());
            stream0 = collection.stream();
        }

        Object[] stream0R = stream0.toArray();

        ArrayList<Tuple2<Data<Type0, Type1>, Input>> list0 = new ArrayList<>();

        final Function<Input, Type0> get0Pivot_ = javaExecutor.getCompiler().compile(this.get0Pivot);
        final Function<Input, Type1> get0Ref_ = javaExecutor.getCompiler().compile(this.get0Ref);

        for (int i = 0; i < stream0R.length; i++) {
            list0.add(new Tuple2<>(new extractData<>(get0Pivot_, get0Ref_).call((Input) stream0R[i]), (Input) stream0R[i]));
        }

        Collections.sort(list0, new DataComparator<>(list1ASC, list1ASCSec));

        long partCount = list0.size();

        // Give unique ID for rdd1
        for (int i = 0; i < partCount; i++) {
            list0.get(i)._1().setRowID(i);
        }

        ArrayList<Tuple2<Input, Input>> result = new BitSetJoin<Type0, Type1, Input>(list1ASC, list2ASC,
                list1ASCSec, list2ASCSec, equalReverse, true, cond0).call(list0, list0);

        ArrayList<org.apache.wayang.basic.data.Tuple2<Input, Input>> result2 = new ArrayList<>();
        for (Tuple2<Input, Input> t : result) {
            result2.add(new org.apache.wayang.basic.data.Tuple2<Input, Input>(t._1(), t._2()));
        }

        outputChannel.<org.apache.wayang.basic.data.Tuple2<Input, Input>>accept(result2.stream());

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new JavaIESelfJoinOperator<Type0, Type1, Input>(this.getInputType(),
                get0Pivot, cond0, get0Ref, cond1);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
