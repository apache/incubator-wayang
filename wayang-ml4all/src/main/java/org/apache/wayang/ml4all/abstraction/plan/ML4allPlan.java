/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml4all.abstraction.plan;

import org.apache.wayang.api.DataQuantaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.PredicateDescriptor;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.util.WayangCollections;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.ml4all.abstraction.api.*;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.AssignWrapperLocal;
import org.apache.wayang.ml4all.abstraction.plan.wrappers.*;

import java.util.ArrayList;
import java.util.Collection;

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

    /*
     * Return the last state of ML4allGlobalVars that contains the model
     */
    public ML4allModel execute(String inputFileUrl, WayangContext wayangContext) {

        wayangContext.getConfiguration().setProperty("wayang.core.optimizer.reoptimize", "false");

        JavaPlanBuilder javaPlanBuilder = new JavaPlanBuilder(wayangContext)
                .withUdfJar(ReflectionUtils.getDeclaringJar(ML4allModel.class))
                .withUdfJar(ReflectionUtils.getDeclaringJar(JavaPlatform.class))
                .withJobName("ML4all plan");

        ML4allModel vars = new ML4allModel();
        localStageOp.staging(vars);
        ArrayList<ML4allModel> broadcastModel = new ArrayList<>(1);
        broadcastModel.add(vars);
        final DataQuantaBuilder<?, ML4allModel> modelBuilder = javaPlanBuilder.loadCollection(broadcastModel).withName("init model");

        final DataQuantaBuilder transformBuilder = javaPlanBuilder
                .readTextFile(inputFileUrl).withName("source")
                .mapPartitions(new TransformPerPartitionWrapper(transformOp)).withName("transform");

        Collection<ML4allModel> results =
                modelBuilder.doWhile((PredicateDescriptor.SerializablePredicate<Collection<Double>>) collection ->
                        new LoopCheckWrapper<>(loopOp).apply(collection.iterator().next()), model -> {

                    DataQuantaBuilder convergenceDataset;
                    DataQuantaBuilder<?, ML4allModel> newModel;

                    DataQuantaBuilder sampledData;
                    if (hasSample()) //sample data first
                        sampledData = transformBuilder
                                .sample(sampleOp.sampleSize()).withSampleMethod(sampleOp.sampleMethod()).withDatasetSize(datasetsize).withBroadcast(model, "model");
                    else //sampled data is entire dataset
                        sampledData = transformBuilder;

                    if (isUpdateLocal()) { //eg., for GD
                        DataQuantaBuilder newWeights = sampledData
                                .map(new ComputeWrapper<>(computeOp)).withBroadcast(model, "model").withName("compute")
                                .reduce(new AggregateWrapper<>(computeOp)).withName("reduce")
                                .map(new UpdateLocalWrapper(updateLocalOp)).withBroadcast(model, "model").withName("update");

                        newModel = newWeights
                                .map(new AssignWrapperLocal(updateLocalOp)).withName("assign")
                                .withBroadcast(model, "model");

                        convergenceDataset = newWeights
                                .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                .withBroadcast(model, "model");

                    } else { //eg., for k-means
                        DataQuantaBuilder listDataset = sampledData
                                .map(new ComputeWrapper<>(computeOp)).withBroadcast(model, "model").withName("compute")
                                .reduceByKey(pair -> ((Tuple2) pair).field0, new AggregateWrapper<>(computeOp)).withName("reduce")
                                .map(new UpdateWrapper(updateOp)).withBroadcast(model, "model").withName("update")
                                .map(t -> {
                                    ArrayList<Tuple2> list = new ArrayList<>(1);
                                    list.add((Tuple2) t);
                                    return list;
                                })
                                .reduce(new ReduceWrapper<>()).withName("global reduce");
                        newModel = listDataset
                                .map(new AssignWrapper(updateOp)).withName("assign")
                                .withBroadcast(model, "model");
                        convergenceDataset = listDataset
                                .map(new LoopConvergenceWrapper(loopOp)).withName("converge")
                                .withBroadcast(model, "model");
                    }

                    return new Tuple<>(newModel, convergenceDataset);
                }).collect();

            return WayangCollections.getSingle(results);
//        }
    }

}
