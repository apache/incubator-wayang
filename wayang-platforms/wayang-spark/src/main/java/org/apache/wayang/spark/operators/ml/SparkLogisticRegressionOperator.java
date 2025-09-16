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
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.LogisticRegressionOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.model.SparkMLModel;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.*;

public class SparkLogisticRegressionOperator extends LogisticRegressionOperator implements SparkExecutionOperator {

    private static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField(Attr.LABEL, DataTypes.DoubleType, false),
            DataTypes.createStructField(Attr.FEATURES, new VectorUDT(), false)
    });

    private static Dataset<Row> convertToDataFrame(JavaRDD<double[]> features, JavaRDD<Double> labels) {
        JavaPairRDD<double[], Double> zipped = features.zip(labels);
        JavaRDD<Row> rowRDD = zipped.map(e -> RowFactory.create(e._2, Vectors.dense(e._1)));
        return SparkSession.builder().getOrCreate().createDataFrame(rowRDD, SCHEMA);
    }

    public SparkLogisticRegressionOperator(boolean fitIntercept) {
        super(fitIntercept);
    }

    public SparkLogisticRegressionOperator(LogisticRegressionOperator that) {
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

        JavaRDD<double[]> features = featuresInput.provideRdd();
        JavaRDD<Double> labels = labelsInput.provideRdd();

        Dataset<Row> df = convertToDataFrame(features, labels);

        LogisticRegression lr = new LogisticRegression()
                .setFitIntercept(this.fitIntercept)
                .setLabelCol(Attr.LABEL)
                .setFeaturesCol(Attr.FEATURES)
                .setPredictionCol(Attr.PREDICTION);

        LogisticRegressionModel model = lr.fit(df);
        output.accept(Collections.singletonList(new Model(model)));

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    public static class Model implements org.apache.wayang.basic.model.LogisticRegressionModel, SparkMLModel<double[], Double> {

        private static final StructType PREDICT_SCHEMA = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(Attr.FEATURES, new VectorUDT(), false)
        });

        private final LogisticRegressionModel model;

        public Model(LogisticRegressionModel model) {
            this.model = model;
        }

        private static Dataset<Row> convertToDataFrame(JavaRDD<double[]> features) {
            JavaRDD<Row> rows = features.map(f -> RowFactory.create(Vectors.dense(f)));
            return SparkSession.builder().getOrCreate().createDataFrame(rows, PREDICT_SCHEMA);
        }

        @Override
        public JavaRDD<Tuple2<double[], Double>> transform(JavaRDD<double[]> input) {
            Dataset<Row> df = convertToDataFrame(input);
            Dataset<Row> result = model.transform(df);
            return result.toJavaRDD().map(row ->
                    new Tuple2<>(row.getAs(Attr.FEATURES), row.<Double>getAs(Attr.PREDICTION)));
        }

        @Override
        public JavaRDD<Double> predict(JavaRDD<double[]> input) {
            Dataset<Row> df = convertToDataFrame(input);
            Dataset<Row> result = model.transform(df);
            return result.toJavaRDD().map(row -> row.<Double>getAs(Attr.PREDICTION));
        }
    }
}
