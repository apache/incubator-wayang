/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spatial.operators.java;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.SpatialJoinOperator;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.spatial.function.JtsSpatialPredicate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Stream;

public class JavaSpatialJoinOperator<InputType0, InputType1>
        extends SpatialJoinOperator<InputType0, InputType1>
        implements JavaExecutionOperator {

    public JavaSpatialJoinOperator(TransformationDescriptor<InputType0, ? extends SpatialGeometry> keyDescriptor0,
                                   TransformationDescriptor<InputType1, ? extends SpatialGeometry> keyDescriptor1,
                                   DataSetType<InputType0> inputType0,
                                   DataSetType<InputType1> inputType1,
                                   SpatialPredicate predicate) {
        super(keyDescriptor0, keyDescriptor1, inputType0, inputType1, predicate);
    }

    public JavaSpatialJoinOperator(FunctionDescriptor.SerializableFunction<InputType0, ? extends SpatialGeometry> keyExtractor0,
                               FunctionDescriptor.SerializableFunction<InputType1, ? extends SpatialGeometry> keyExtractor1,
                               Class<InputType0> input0Class,
                               Class<InputType1> input1Class,
                               SpatialPredicate predicate) {
        super(keyExtractor0, keyExtractor1, input0Class, input1Class, predicate);
    }


    public JavaSpatialJoinOperator(SpatialJoinOperator<InputType0, InputType1> that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final Function<InputType0, ? extends SpatialGeometry> keyExtractor0 =
                javaExecutor.getCompiler().compile(this.keyDescriptor0);
        final Function<InputType1, ? extends SpatialGeometry> keyExtractor1 =
                javaExecutor.getCompiler().compile(this.keyDescriptor1);

        final Stream<InputType0> leftStream =
                ((org.apache.wayang.java.channels.JavaChannelInstance) inputs[0])
                        .provideStream();
        final Stream<InputType1> rightStream =
                ((org.apache.wayang.java.channels.JavaChannelInstance) inputs[1])
                        .provideStream();

        JtsSpatialPredicate predicate = JtsSpatialPredicate.of(this.predicateType);

        STRtree index = new STRtree();

        rightStream.forEach(v1 -> {
            WayangGeometry wGeom = (WayangGeometry) keyExtractor1.apply(v1);
            Geometry geom = (wGeom == null) ? null : wGeom.getGeometry();
            if (geom != null) {
                index.insert(geom.getEnvelopeInternal(), new AbstractMap.SimpleEntry<>(v1, geom));
            }
        });

        index.build();

        final Stream<Tuple2<InputType0, InputType1>> joinStream = leftStream.flatMap(v0 -> {
            Geometry geom0 = Optional.ofNullable((WayangGeometry) keyExtractor0.apply(v0))
                    .map(WayangGeometry::getGeometry).orElse(null);
            if (geom0 == null) return Stream.empty();

            List<Map.Entry<InputType1, Geometry>> candidates = index.query(geom0.getEnvelopeInternal());

            return candidates.stream()
                    .filter(e -> predicate.test(geom0, e.getValue()))
                    .map(e -> new Tuple2<>(v0, e.getKey()));
        });

        // Push the result into the output channel.
        ((org.apache.wayang.java.channels.StreamChannel.Instance) outputs[0]).accept(joinStream);

        // Use the standard lazy-execution lineage modeling.
        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index <= this.getNumInputs() || (index == 0 && this.getNumInputs() == 0);
        if (this.getInput(index).isBroadcast()) return Collections.singletonList(CollectionChannel.DESCRIPTOR);
        return Arrays.asList(CollectionChannel.DESCRIPTOR, StreamChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }
}
