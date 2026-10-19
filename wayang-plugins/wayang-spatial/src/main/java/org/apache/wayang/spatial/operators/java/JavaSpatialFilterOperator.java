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

package org.apache.wayang.spatial.operators.java;

import org.apache.wayang.basic.operators.SpatialFilterOperator;
import org.apache.wayang.core.api.spatial.SpatialGeometry;
import org.apache.wayang.core.api.spatial.SpatialPredicate;
import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.JavaChannelInstance;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.apache.wayang.spatial.function.JtsSpatialPredicate;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Java implementation of the {@link SpatialFilterOperator}.
 */
public class JavaSpatialFilterOperator<Type>
        extends SpatialFilterOperator<Type>
        implements JavaExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     */
    public JavaSpatialFilterOperator(SpatialPredicate relation,
                                     FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                     DataSetType<Type> inputClassDatasetType,
                                     SpatialGeometry geometry) {
        super(relation, keyExtractor, inputClassDatasetType, geometry);
    }

    public JavaSpatialFilterOperator(SpatialFilterOperator<Type> that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        final Predicate<Type> filterPredicate = this.buildSpatialPredicate(javaExecutor);
        ((StreamChannel.Instance) outputs[0]).accept(
                ((JavaChannelInstance) inputs[0]).<Type>provideStream().filter(filterPredicate)
        );

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    private Predicate<Type> buildSpatialPredicate(JavaExecutor javaExecutor) {
        WayangGeometry wRef = (WayangGeometry) this.referenceGeometry;
        final Geometry reference = wRef.getGeometry();
        final Function<Type, ? extends SpatialGeometry> keyExtractor = javaExecutor.getCompiler().compile(this.keyDescriptor);
        JtsSpatialPredicate predicate = JtsSpatialPredicate.of(this.predicateType);

        return input -> predicate.test(((WayangGeometry) keyExtractor.apply(input)).getGeometry(), reference);
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
