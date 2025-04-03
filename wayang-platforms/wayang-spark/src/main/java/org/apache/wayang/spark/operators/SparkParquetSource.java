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

package org.apache.wayang.spark.operators;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.spark.channels.RddChannel;
import org.apache.wayang.spark.execution.SparkExecutor;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Provides a {@link Collection} to a Spark job.
 */
public class SparkParquetSource extends ParquetSource implements SparkExecutionOperator {

    public SparkParquetSource(String inputUrl, String[] projection, String... columnNames) {
        super(inputUrl, projection, columnNames);
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public SparkParquetSource(ParquetSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        RddChannel.Instance output = (RddChannel.Instance) outputs[0];

        Dataset<Row> table = sparkExecutor.ss.read().parquet(this.getInputUrl().trim());

        // Reads a projection, if any (loads the complete file if no projection defined)
        String[] projection = this.getProjection();
        if (projection != null && projection.length > 0) {
            table = table.selectExpr(projection);
        }

        // Wrap dataset into a JavaRDD and convert Row's to Record's
        JavaRDD<Record> rdd = table.toJavaRDD().map(row -> {
            List<Object> values = IntStream.range(0, row.size())
                    .mapToObj(row::get)
                    .collect(Collectors.toList());
            return new Record(values);
        });
        this.name(rdd);
        output.accept(rdd, sparkExecutor);

        ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.spark.parquetsource.load.prepare", sparkExecutor.getConfiguration()
        ));
        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.spark.parquetsource.load.main", sparkExecutor.getConfiguration()
        ));
        output.getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.spark.parquetsource.load.prepare", "wayang.spark.parquetsource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
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
