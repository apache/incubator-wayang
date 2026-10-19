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

package org.apache.wayang.spatial.operators.spark;

import org.apache.sedona.core.spatialOperator.RangeQuery;
import org.apache.sedona.core.spatialRDD.SpatialRDD;
import org.apache.spark.api.java.JavaRDD;
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
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.BroadcastChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spatial.data.WayangGeometry;
import org.locationtech.jts.geom.Geometry;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Spark implementation of the {@link SpatialFilterOperator}.
 */
public class SparkSpatialFilterOperator<Type>
        extends SpatialFilterOperator<Type>
        implements SparkExecutionOperator {

    /**
     * Creates a new instance.
     *
     * @param relation the type of spatial filter (e.g., "INTERSECTS", "CONTAINS", "WITHIN")
     *
     */
    public SparkSpatialFilterOperator(SpatialPredicate relation,
                                      FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor,
                                      DataSetType<Type> inputClassDatasetType,
                                      SpatialGeometry geometry) {
        super(relation, keyExtractor, inputClassDatasetType, geometry);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkSpatialFilterOperator(SpatialFilterOperator that) {
        super(that);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        // Register Sedona JAR with Spark executors if running in cluster mode.
        if (!sparkExecutor.sc.isLocal()) {
            String sedonaJar = ReflectionUtils.getDeclaringJar(SpatialRDD.class);
            if (sedonaJar != null) {
                sparkExecutor.sc.addJar(sedonaJar);
            }
        }

        WayangGeometry wRef = (WayangGeometry) this.referenceGeometry;
        final Geometry reference = wRef == null ? null : wRef.getGeometry();
        if (reference == null) {
            throw new IllegalStateException("Reference geometry must not be null for spatial filtering.");
        }

        final JavaRDD<Type> inputRdd = ((RddChannel.Instance) inputs[0]).provideRdd();

        final FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry> keyExtractor =
                (FunctionDescriptor.SerializableFunction<Type, ? extends SpatialGeometry>) this.keyDescriptor.getJavaImplementation();

        // Build an RDD of Geometries where userData = original element (Type)
        final JavaRDD<Geometry> geometryRdd = inputRdd
                .map((Type value) -> {
                    final WayangGeometry wGeom = (WayangGeometry) keyExtractor.apply(value);
                    if (wGeom == null) {
                        return null;
                    }
                    final Geometry geom = wGeom.getGeometry();
                    if (geom != null) {
                        geom.setUserData(value); // keep original object
                    }
                    return geom;
                })
                .filter(Objects::nonNull);

        final SpatialRDD<Geometry> spatialRDD = new SpatialRDD<>();
        spatialRDD.setRawSpatialRDD(geometryRdd);
        spatialRDD.analyze();

        final JavaRDD<Type> outputRdd = this.applySedonaSpatialFilter(spatialRDD, reference);
        this.name(outputRdd);
        ((RddChannel.Instance) outputs[0]).accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }


    private JavaRDD<Type> applySedonaSpatialFilter(SpatialRDD<Geometry> spatialRDD, Geometry reference) {
        final org.apache.sedona.core.spatialOperator.SpatialPredicate predicate = toSedonaPredicate(this.predicateType);

        try {
            final JavaRDD<Geometry> matched =
                    RangeQuery.SpatialRangeQuery(spatialRDD, reference, predicate, false);

            // Extract original input object from userData
            return matched.map(geom -> (Type) geom.getUserData());
        } catch (Exception e) {
            throw new RuntimeException("Sedona range query failed for spatial filter.", e);
        }
    }

    private org.apache.sedona.core.spatialOperator.SpatialPredicate toSedonaPredicate(SpatialPredicate predicateType) {
        return switch (predicateType) {
            case INTERSECTS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.INTERSECTS;
            case CONTAINS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.CONTAINS;
            case WITHIN -> org.apache.sedona.core.spatialOperator.SpatialPredicate.WITHIN;
            case TOUCHES -> org.apache.sedona.core.spatialOperator.SpatialPredicate.TOUCHES;
            case OVERLAPS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.OVERLAPS;
            case CROSSES -> org.apache.sedona.core.spatialOperator.SpatialPredicate.CROSSES;
            case EQUALS -> org.apache.sedona.core.spatialOperator.SpatialPredicate.EQUALS;
            default -> throw new IllegalStateException("Unsupported spatial filter predicate: " + predicateType);
        };
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.spark.spatialfilter.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        if (index == 0) {
            return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
        } else {
            return Collections.singletonList(BroadcastChannel.DESCRIPTOR);
        }
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

}
