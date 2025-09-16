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


package org.apache.wayang.spark.operators.ml;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.LinearSVC;
import org.apache.spark.ml.classification.LinearSVCModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.wayang.basic.operators.LinearSVCOperator;
import org.apache.wayang.basic.model.SVMModel;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;
import org.apache.wayang.spark.model.SparkMLModel;

import java.util.*;

public class SparkLinearSVCOperator extends LinearSVCOperator implements SparkExecutionOperator {

    private static final String FEATURES = "features";
    private static final String LABEL = "label";

    private static final StructType SCHEMA = new StructType(new StructField[]{
            new StructField(FEATURES, new VectorUDT(), false, Metadata.empty()),
            new StructField(LABEL, DataTypes.DoubleType, false, Metadata.empty())
    });

    public SparkLinearSVCOperator(int maxIter, double regParam) {
        super(maxIter, regParam);
    }

    public SparkLinearSVCOperator(LinearSVCOperator that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        RddChannel.Instance featuresInput = (RddChannel.Instance) inputs[0];
        RddChannel.Instance labelsInput = (RddChannel.Instance) inputs[1];
        CollectionChannel.Instance output = (CollectionChannel.Instance) outputs[0];

        JavaRDD<double[]> featuresRdd = featuresInput.provideRdd();
        JavaRDD<Double> labelsRdd = labelsInput.provideRdd();

        JavaRDD<Row> rows = featuresRdd.zip(labelsRdd).map(tuple ->
                RowFactory.create(Vectors.dense(tuple._1), tuple._2)
        );

        Dataset<Row> trainingData = SparkSession.builder()
                .getOrCreate()
                .createDataFrame(rows, SCHEMA);

        LinearSVC lsvc = new LinearSVC()
                .setMaxIter(this.getMaxIter())
                .setRegParam(this.getRegParam())
                .setFeaturesCol(FEATURES)
                .setLabelCol(LABEL);

        LinearSVCModel sparkModel = lsvc.fit(trainingData);

        output.accept(Collections.singletonList(new Model(sparkModel)));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    public static class Model implements SVMModel, SparkMLModel<double[], Double> {

        private final LinearSVCModel model;

        public Model(LinearSVCModel model) {
            this.model = model;
        }

        @Override
        public double predict(double[] features) {
            return model.predict(Vectors.dense(features));
        }

        @Override
        public JavaRDD<Double> predict(JavaRDD<double[]> input) {
            JavaRDD<Row> rows = input.map(f -> RowFactory.create(Vectors.dense(f)));
            StructType schema = new StructType(new StructField[]{
                    new StructField(FEATURES, new VectorUDT(), false, Metadata.empty())
            });
            Dataset<Row> df = SparkSession.builder().getOrCreate().createDataFrame(rows, schema);
            Dataset<Row> predictions = model.transform(df);
            return predictions.select("prediction").toJavaRDD().map(row -> row.getDouble(0));
        }

        @Override
        public JavaRDD<Tuple2<double[], Double>> transform(JavaRDD<double[]> input) {
            JavaRDD<Row> rows = input.map(f -> RowFactory.create(Vectors.dense(f)));
            StructType schema = new StructType(new StructField[]{
                    new StructField(FEATURES, new VectorUDT(), false, Metadata.empty())
            });
            Dataset<Row> df = SparkSession.builder().getOrCreate().createDataFrame(rows, schema);
            Dataset<Row> predictions = model.transform(df);
            return predictions.toJavaRDD().map(row ->
                    new Tuple2<>(((org.apache.spark.ml.linalg.Vector) row.getAs(FEATURES)).toArray(),
                            row.getAs("prediction")));
        }
    }
}
