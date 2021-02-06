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

package org.apache.wayang.basic.operators;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimator;
import org.apache.wayang.core.plan.wayangplan.EstimationContextProperty;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.UnaryToUnaryOperator;
import org.apache.wayang.core.types.DataSetType;

import java.util.Optional;

/**
 * {@link Operator} for the PageRank algorithm. It takes as input a list of directed edges, whereby each edge
 * is represented as {@code (source vertex ID, target vertex ID)} tuple. Its output are the page ranks, codified
 * as {@code (vertex ID, page rank)} tuples.
 */
public class PageRankOperator extends UnaryToUnaryOperator<Tuple2<Long, Long>, Tuple2<Long, Float>> {

    public static final double DEFAULT_DAMPING_FACTOR = 0.85d;

    public static final ProbabilisticDoubleInterval DEFAULT_GRAPH_DENSITIY = new ProbabilisticDoubleInterval(.0001d, .5d, .5d);

    @EstimationContextProperty
    protected final Integer numIterations;

    protected final float dampingFactor;

    protected final ProbabilisticDoubleInterval graphDensity;

    /**
     * Creates a new instance.
     *
     * @param numIterations the number of PageRank iterations that this instance should perform
     */
    public PageRankOperator(Integer numIterations) {
        this(numIterations, DEFAULT_DAMPING_FACTOR, DEFAULT_GRAPH_DENSITIY);
    }

    /**
     * Creates a new instance.
     *
     * @param numIterations the number of PageRank iterations that this instance should perform
     */
    public PageRankOperator(Integer numIterations, Double dampingFactor, ProbabilisticDoubleInterval graphDensitiy) {
        super(DataSetType.createDefaultUnchecked(Tuple2.class),
                DataSetType.createDefaultUnchecked(Tuple2.class),
                false);
        this.numIterations = numIterations;
        this.dampingFactor = dampingFactor.floatValue();
        this.graphDensity = graphDensitiy;
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public PageRankOperator(PageRankOperator that) {
        super(that);
        this.numIterations = that.getNumIterations();
        this.dampingFactor = that.dampingFactor;
        this.graphDensity = that.graphDensity;
    }

    public int getNumIterations() {
        return numIterations;
    }

    public float getDampingFactor() {
        return dampingFactor;
    }

    public ProbabilisticDoubleInterval getGraphDensity() {
        return graphDensity;
    }

    @Override
    public Optional<CardinalityEstimator> createCardinalityEstimator(int outputIndex, Configuration configuration) {
        switch (outputIndex) {
            case 0:
                return Optional.of((optimizationContext, inputEstimates) -> {
                    assert inputEstimates.length == 1;
                    return new CardinalityEstimate(
                            calculateNumVertices(inputEstimates[0].getLowerEstimate(), PageRankOperator.this.graphDensity.getUpperEstimate()),
                            calculateNumVertices(inputEstimates[0].getUpperEstimate(), PageRankOperator.this.graphDensity.getLowerEstimate()),
                            inputEstimates[0].getCorrectnessProbability() * PageRankOperator.this.graphDensity.getCorrectnessProbability()
                    );
                });
            default:
                throw new IllegalArgumentException(String.format("%s does not have an OutputSlot with index %d.", this, outputIndex));
        }
    }

    /**
     * Calculate the number of vertices in a graph with a given number of edges and density.
     *
     * @param numEdges number of edges in the graph
     * @param density  the graph density
     * @return the number of vertices in the graph
     */
    private static long calculateNumVertices(long numEdges, double density) {
        return density == 0 ? 0L : Math.round(0.5d + Math.sqrt(0.25 + 2 * numEdges / density));
    }


}
