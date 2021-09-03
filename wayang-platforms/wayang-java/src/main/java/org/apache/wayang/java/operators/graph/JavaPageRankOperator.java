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

package org.apache.wayang.java.operators.graph;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.PageRankOperator;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.CollectionChannel;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.operators.JavaExecutionOperator;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Java implementation of the {@link PageRankOperator}.
 */
public class JavaPageRankOperator extends PageRankOperator implements JavaExecutionOperator {

    public JavaPageRankOperator(int numIterations) {
        super(numIterations);
    }

    public JavaPageRankOperator(PageRankOperator that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        final Collection<Tuple2<Long, Long>> edges = input.provideCollection();
        final Map<Long, Float> pageRanks = this.pageRank(edges);
        final Stream<Tuple2<Long, Float>> pageRankStream = pageRanks.entrySet().stream().map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()));

        output.accept(pageRankStream);

        return ExecutionOperator.modelQuasiEagerExecution(inputs, outputs, operatorContext);
    }

    /**
     * Execute the PageRank algorithm.
     *
     * @param edgeDataSet edges of a graph
     * @return the page ranks
     */
    //TODO: change for efficient map
    private Map<Long, Float> pageRank(Collection<Tuple2<Long, Long>> edgeDataSet) {
        // Get the degress of all vertices and make sure we collect *all* vertices.
        //TODO: change for efficient map
        HashMap<Long, Integer> degrees = new HashMap<>();
        for (Tuple2<Long, Long> edge : edgeDataSet) {
            this.adjustOrPutValue(degrees, edge.field0, 1, 1, Integer::sum);
            this.adjustOrPutValue(degrees, edge.field0, 0, 0, Integer::sum);
        }
        int numVertices = degrees.size();
        float initialRank = 1f / numVertices;
        float dampingRank = (1 - this.dampingFactor) / numVertices;

        // Initialize the rank map.
        //TODO: change for efficient map
        HashMap<Long, Float> initialRanks = new HashMap<>();
        degrees.forEach( (k, v) -> {
            initialRanks.putIfAbsent(k, initialRank);
        });

        HashMap<Long, Float> currentRanks = initialRanks;
        for (int iteration = 0; iteration < this.getNumIterations(); iteration++) {
            // Add the damping first.
            //TODO: change for efficient map
            HashMap<Long, Float> newRanks = new HashMap<Long, Float>(currentRanks.size());
            degrees.forEach( (k, v) -> {
                newRanks.putIfAbsent(k, dampingRank);
            });

            // Now add the other ranks.
            for (Tuple2<Long, Long> edge : edgeDataSet) {
                final long sourceVertex = edge.field0;
                final long targetVertex = edge.field1;
                final int degree = degrees.get(sourceVertex);
                final float currentRank = currentRanks.get(sourceVertex);
                final float partialRank = this.dampingFactor * currentRank / degree;
                this.adjustOrPutValue(newRanks, targetVertex, partialRank, partialRank, Float::sum);
            }

            currentRanks = newRanks;
        }

        return currentRanks;
    }

    /**
     * simulate the process on the Trove4j library
     * @param key key to modify on the map
     * @param default_value default value in the case of not key
     * @param correction element to add the array in the case of the key exist
     */
    private <T> void adjustOrPutValue(Map<Long, T> map, Long key, T default_value, T correction, BiFunction<T, T, T> update){
        if(map.containsKey(key)){
            T value = map.get(key);
            map.replace(key, update.apply(value, correction) );
        }else{
            map.put(key, default_value);
        }
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "wayang.java.pagerank.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(CollectionChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        assert index == 0;
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
