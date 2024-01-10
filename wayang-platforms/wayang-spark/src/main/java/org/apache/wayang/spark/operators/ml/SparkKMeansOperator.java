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
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.KMeansOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.operators.SparkExecutionOperator;

import java.util.*;

public class SparkKMeansOperator extends KMeansOperator implements SparkExecutionOperator {

    public SparkKMeansOperator(int k) {
        super(k);
    }

    public SparkKMeansOperator(KMeansOperator that) {
        super(that);
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        // TODO need DataFrameChannel?
        return Arrays.asList(RddChannel.UNCACHED_DESCRIPTOR, RddChannel.CACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        // TODO need DataFrameChannel?
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumInputs();

        final RddChannel.Instance input = (RddChannel.Instance) inputs[0];
        final RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        final JavaRDD<double[]> inputRdd = input.provideRdd();
        final JavaRDD<Data> dataRdd = inputRdd.map(Data::new);
        final Dataset<Row> df = SparkSession.builder().getOrCreate().createDataFrame(dataRdd, Data.class);
        final KMeansModel model = new KMeans()
                .setK(this.k)
                .fit(df);

        final Dataset<Row> transform = model.transform(df);
        final JavaRDD<Tuple2<double[], Integer>> outputRdd = transform.toJavaRDD()
                .map(row -> new Tuple2<>(((Vector) row.get(0)).toArray(), (Integer) row.get(1)));

        this.name(outputRdd);
        output.accept(outputRdd, sparkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    // TODO support fit and transform

    @Override
    public boolean containsAction() {
        return false;
    }

    public static class Data {
        private final Vector features;


        public Data(Vector features) {
            this.features = features;
        }

        public Data(double[] features) {
            this.features = Vectors.dense(features);
        }

        public Vector getFeatures() {
            return features;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "features=" + features +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Data)) return false;
            Data data = (Data) o;
            return Objects.equals(features, data.features);
        }

        @Override
        public int hashCode() {
            return Objects.hash(features);
        }
    }
}
