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
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.wayang.basic.operators.TimeSeriesDecisionTreeRegressionOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.*;

public class SparkTimeSeriesDecisionTreeRegressionOperator
        extends TimeSeriesDecisionTreeRegressionOperator
        implements SparkExecutionOperator {

    private static final String FEATURES = "features";
    private static final String LABEL = "label";

    private static final StructType SCHEMA = new StructType(new StructField[]{
            new StructField(FEATURES, new VectorUDT(), false, Metadata.empty()),
            new StructField(LABEL, DataTypes.DoubleType, false, Metadata.empty())
    });

    public SparkTimeSeriesDecisionTreeRegressionOperator(int lagWindowSize, int maxDepth, int minInstancesPerNode) {
        super(lagWindowSize, maxDepth, minInstancesPerNode);
    }

    public SparkTimeSeriesDecisionTreeRegressionOperator(TimeSeriesDecisionTreeRegressionOperator that) {
        super(that);
    }

    private static Dataset<Row> createLaggedData(JavaRDD<double[]> seriesRdd, int lag) {
        JavaRDD<Row> rows = seriesRdd.flatMap(series -> {
            List<Row> result = new ArrayList<>();
            for (int i = lag; i < series.length; i++) {
                double[] input = Arrays.copyOfRange(series, i - lag, i);
                double label = series[i];
                result.add(RowFactory.create(Vectors.dense(input), label));
            }
            return result.iterator();
        });

        return SparkSession.builder().getOrCreate().createDataFrame(rows, SCHEMA);
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

        JavaRDD<double[]> timeSeriesRdd = featuresInput.provideRdd(); // 1D time series as array
        int lag = this.getLagWindowSize();

        Dataset<Row> trainingData = createLaggedData(timeSeriesRdd, lag);

        DecisionTreeRegressor dt = new DecisionTreeRegressor()
                .setLabelCol(LABEL)
                .setFeaturesCol(FEATURES)
                .setMaxDepth(this.getMaxDepth())
                .setMinInstancesPerNode(this.getMinInstancesPerNode());

        DecisionTreeRegressionModel model = dt.fit(trainingData);

        // Predict next values for each feature vector used in training
        Dataset<Row> predictions = model.transform(trainingData);
        JavaRDD<Double> predictedValues = predictions.toJavaRDD().map(row -> row.getDouble(2)); // prediction col is at index 2

        output.accept(predictedValues.collect());

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }
}
