/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.wayang.ml4all.abstraction.plan;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.SampleOperator;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.ml4all.abstraction.api.*;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.AssignWrapperLocal;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.*;
import org.apache.wayang.spark.Spark;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Wayang physical plan for ML4all algorithms
 */

public class ML4allPlan {

    Transform transformOp;
    Class transformIn = String.class; //default value: String
    Class transformOut = double[].class; //default value: double[]

    LocalStage localStageOp;

    Compute computeOp;
    Class computeOutKey = Integer.class; //default: integer
    Class computeOutValue = double[].class; //default: double[]

    Update updateOp;
    UpdateLocal updateLocalOp;

    Loop loopOp;
    Class loopConvergeOut;

    Sample sampleOp;

    long datasetsize;

    public void setTransformOp(Transform transformOp) {
        this.transformOp = transformOp;
    }

    public void setTransformInput(Class transformIn) {
        this.transformIn = transformIn;
    }

    public void setTransformOutput(Class transformOut) {
        this.transformOut = transformOut;
    }

    public void setLocalStage(LocalStage stageOp) {
        this.localStageOp = stageOp;
    }

    public void setComputeOp(Compute computeOp) {
        this.computeOp = computeOp;
    }

    public void setComputeOutput(Class key, Class value) {
        this.computeOutKey = key;
        this.computeOutValue = value;
    }

    public void setSampleOp(Sample sampleOp) {
        this.sampleOp = sampleOp;
    }

    public void setUpdateOp(Update updateOp) {
        this.updateOp = updateOp;
    }

    public void setUpdateLocalOp(UpdateLocal updateLocalOp) {
        this.updateLocalOp = updateLocalOp;
    }

    public void setLoopOp(Loop loopOp) { this.loopOp = loopOp; }

    public void setLoopConvergeOutput(Class loopConvergeOut) { this.loopConvergeOut = loopConvergeOut; }

    public boolean isUpdateLocal() {
        return updateLocalOp != null;
    }

    public boolean hasSample() { return sampleOp != null; }

    public void setDatasetsize(long datasetsize) {
        this.datasetsize = datasetsize;
    }


    public WayangContext initiateWayangContext(Platforms platform, String propertiesFile) {
        // Instantiate Wayang and activate the backend.
        Configuration conf;
        if (propertiesFile == null)
            conf = new Configuration();
        else
            conf = new Configuration(propertiesFile);
        WayangContext wayangContext = new WayangContext(conf);

        switch (platform) {
            case SPARK:
                wayangContext.with(Spark.basicPlugin());
                break;
            case JAVA:
                wayangContext.with(Java.basicPlugin());
                break;
            case SPARK_JAVA:
                wayangContext.with(Java.basicPlugin());
                wayangContext.with(Spark.basicPlugin());
                break;
            default:
                System.err.format("Unknown platform: \"%s\"\n", platform);
                System.exit(3);
                return null;
        }
        wayangContext.getConfiguration().setProperty("wayang.core.optimizer.reoptimize", "false");
        return wayangContext;
    }

    /*
     * Return the last state of ML4allContext that contains the model
     */
    public ML4allContext execute(String inputFileUrl, Platforms platform, String propertiesFile) {
        // Instantiate Wayang and activate the backend.
        WayangContext wayangContext = initiateWayangContext(platform, propertiesFile);

        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext)
                .withUdfJar(ReflectionUtils.getDeclaringJar(ML4allContext.class))
                .withUdfJar(ReflectionUtils.getDeclaringJar(JavaPlatform.class))
                .withJobName("ML4all plan");

        ML4allContext context = new ML4allContext();
        localStageOp.staging(context);
        ArrayList<ML4allContext> broadcastContext = new ArrayList<>(1);
        broadcastContext.add(context);
        final DataQuantaBuilder<?, ML4allContext> contextBuilder = javaPlanBuilder.loadCollection(broadcastContext).withName("init context");

        if (platform.equals(Platforms.SPARK_JAVA)) {
            final DataQuantaBuilder transformBuilder = javaPlanBuilder
                    .readTextFile(inputFileUrl).withName("source")
                    .withTargetPlatform(Spark.platform())
                    .mapPartitions(new TransformPerPartitionWrapper(transformOp)).withName("transform")
                    .withTargetPlatform(Spark.platform());

            Collection<ML4allContext> results =
                    contextBuilder.doWhile((PredicateDescriptor.SerializablePredicate<Collection<Double>>) collection ->
                                    new LoopCheckWrapper<>(loopOp).apply(collection.iterator().next()), ctx -> {

                                DataQuantaBuilder convergenceDataset; //TODO: don't restrict the convergence value to be double
                                DataQuantaBuilder<?, ML4allContext> newContext;

                                DataQuantaBuilder sampledData;
                                if (hasSample()) //sample data first
                                    sampledData = transformBuilder
                                            .customOperator(new SampleOperator(iter -> sampleOp.sampleSize(), DataSetType.createDefault(Void.class), sampleOp.sampleMethod(), iter -> sampleOp.seed(iter)));
                                else //sampled data is entire dataset
                                    sampledData = transformBuilder;

                                if (isUpdateLocal()) { //eg., for GD
                                    DataQuantaBuilder newWeights = sampledData
                                            .map(new ComputeWrapper<>(computeOp)).withBroadcast(ctx, "context").withName("compute")
                                            .reduce(new AggregateWrapper<>(computeOp)).withName("reduce")
                                            .map(new UpdateLocalWrapper(updateLocalOp)).withBroadcast(ctx, "context").withName("update").withTargetPlatform(Java.platform());

                                    newContext = newWeights
                                            .map(new AssignWrapperLocal(updateLocalOp)).withName("assign")
                                            .withBroadcast(ctx, "context")
                                            .withTargetPlatform(Java.platform());
                                    convergenceDataset = newWeights
                                            .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                            .withBroadcast(ctx, "context")
                                            .withTargetPlatform(Java.platform());
                                } else { //eg., for k-means
                                    DataQuantaBuilder listDataset = sampledData
                                            .map(new ComputeWrapper<>(computeOp)).withBroadcast(ctx, "context").withName("compute")
                                            .reduceByKey(pair -> ((Tuple2) pair).field0, new AggregateWrapper<>(computeOp)).withName("reduce")
                                            .map(new UpdateWrapper(updateOp)).withBroadcast(ctx, "context").withName("update")
                                            .map(t -> {
                                                ArrayList<Tuple2> list = new ArrayList<>(1);
                                                list.add((Tuple2) t);
                                                return list;
                                            })
                                            .reduce(new ReduceWrapper<>()).withName("global reduce")
                                            .withTargetPlatform(Spark.platform());
                                    newContext = listDataset
                                            .map(new AssignWrapper(updateOp)).withName("assign")
                                            .withBroadcast(ctx, "context")
                                            .withTargetPlatform(Java.platform());
                                    convergenceDataset = listDataset
                                            .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                            .withBroadcast(ctx, "context")
                                            .withTargetPlatform(Java.platform());
                                }

                                return new Tuple<>(newContext, convergenceDataset);
                            }).withTargetPlatform(Java.platform())
                            .collect();
            return WayangCollections.getSingle(results);
        }
        else {
            final DataQuantaBuilder transformBuilder = javaPlanBuilder
                    .readTextFile(inputFileUrl).withName("source")
                    .mapPartitions(new TransformPerPartitionWrapper(transformOp)).withName("transform");

            Collection<ML4allContext> results =
                    contextBuilder.doWhile((PredicateDescriptor.SerializablePredicate<Collection<Double>>) collection ->
                            new LoopCheckWrapper<>(loopOp).apply(collection.iterator().next()), ctx -> {

                        DataQuantaBuilder convergenceDataset;
                        DataQuantaBuilder<?, ML4allContext> newContext;

                        DataQuantaBuilder sampledData;
                        if (hasSample()) //sample data first
                            sampledData = transformBuilder
                                    .sample(sampleOp.sampleSize()).withSampleMethod(sampleOp.sampleMethod()).withDatasetSize(datasetsize).withBroadcast(ctx, "context");
                        else //sampled data is entire dataset
                            sampledData = transformBuilder;

                        if (isUpdateLocal()) { //eg., for GD
                            DataQuantaBuilder newWeights = sampledData
                                    .map(new ComputeWrapper<>(computeOp)).withBroadcast(ctx, "context").withName("compute")
                                    .reduce(new AggregateWrapper<>(computeOp)).withName("reduce")
                                    .map(new UpdateLocalWrapper(updateLocalOp)).withBroadcast(ctx, "context").withName("update");

                            newContext = newWeights
                                    .map(new AssignWrapperLocal(updateLocalOp)).withName("assign")
                                    .withBroadcast(ctx, "context");

                            convergenceDataset = newWeights
                                    .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                    .withBroadcast(ctx, "context");

                        } else { //eg., for k-means
                            DataQuantaBuilder listDataset = sampledData
                                    .map(new ComputeWrapper<>(computeOp)).withBroadcast(ctx, "context").withName("compute")
                                    .reduceByKey(pair -> ((Tuple2) pair).field0, new AggregateWrapper<>(computeOp)).withName("reduce")
                                    .map(new UpdateWrapper(updateOp)).withBroadcast(ctx, "context").withName("update")
                                    .map(t -> {
                                        ArrayList<Tuple2> list = new ArrayList<>(1);
                                        list.add((Tuple2) t);
                                        return list;
                                    })
                                    .reduce(new ReduceWrapper<>()).withName("global reduce");
                            newContext = listDataset
                                    .map(new AssignWrapper(updateOp)).withName("assign")
                                    .withBroadcast(ctx, "context");
                            convergenceDataset = listDataset
                                    .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                    .withBroadcast(ctx, "context");
                        }

                        return new Tuple<>(newContext, convergenceDataset);
                    }).collect();

            return WayangCollections.getSingle(results);
        }
    }

}
