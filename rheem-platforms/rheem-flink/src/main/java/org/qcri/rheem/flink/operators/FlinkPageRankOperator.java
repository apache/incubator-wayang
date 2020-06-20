package org.qcri.rheem.flink.operators;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.flink.channels.DataSetChannel;
import org.qcri.rheem.flink.execution.FlinkExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

/**
 * Flink implementation of the {@link PageRankOperator}.
 */
public class FlinkPageRankOperator extends PageRankOperator implements FlinkExecutionOperator  {
    private static final float DAMPENING_FACTOR = 0.85f;
    private static final float EPSILON = 0.001f;

    public FlinkPageRankOperator(Integer numIterations) {
        super(numIterations);
    }

    public FlinkPageRankOperator(PageRankOperator pageRankOperator) {
        super(pageRankOperator);
    }


    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>
        evaluate(ChannelInstance[] inputs,
                 ChannelInstance[] outputs,
                 FlinkExecutor flinkExecutor,
                 OptimizationContext.OperatorContext operatorContext
    ) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final DataSetChannel.Instance input = (DataSetChannel.Instance) inputs[0];
        final DataSetChannel.Instance output = (DataSetChannel.Instance) outputs[0];

        MapFunction<org.qcri.rheem.basic.data.Tuple2<Long, Long>, Tuple2<Long,Long>> mapFunction = new MapFunction<org.qcri.rheem.basic.data.Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(org.qcri.rheem.basic.data.Tuple2<Long, Long> longLongTuple2) throws Exception {
                return new Tuple2<>(longLongTuple2.field0, longLongTuple2.field1);
            }
        };

        final DataSet<org.qcri.rheem.basic.data.Tuple2<Long, Long>> dataSetInput = input.provideDataSet();

        final DataSet<Tuple2<Long, Long>> dataSetInputReal = dataSetInput.map(mapFunction);


        FlatMapFunction<Tuple2<Long, Long>, Long> flatMapFunction = new FlatMapFunction<Tuple2<Long, Long>, Long>() {
            @Override
            public void flatMap(Tuple2<Long, Long> longLongTuple2, Collector<Long> collector) throws Exception {
                collector.collect(longLongTuple2.f0);
                collector.collect(longLongTuple2.f1);
            }
        };

        final DataSet<Long> pages = dataSetInputReal.flatMap(flatMapFunction).distinct();

        int numPages = 0;
        try {
            numPages = (int) pages.count();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // get input data
        DataSet<Long> pagesInput = pages;
        DataSet<Tuple2<Long, Long>> linksInput = dataSetInputReal;

        // assign initial rank to pages
        DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
                map(new RankAssigner((1.0d / numPages)));

        // build adjacency list from link input
        DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
                linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

        // set iterative data set
        IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(this.numIterations);

        DataSet<Tuple2<Long, Double>> newRanks = iteration
                // join pages with outgoing edges and distribute rank
                .join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
                // collect and sum ranks
                .groupBy(0).aggregate(SUM, 1)
                // apply dampening factor
                .map(new Dampener(DAMPENING_FACTOR, numPages));

        DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
                newRanks,
                newRanks.join(iteration).where(0).equalTo(0)
                        // termination condition
                        .filter(new EpsilonFilter()));


        final DataSet<org.qcri.rheem.basic.data.Tuple2<Long, Float>> dataSetOutput = finalPageRanks.map(
                new MapFunction<Tuple2<Long, Double>, org.qcri.rheem.basic.data.Tuple2<Long, Float>>() {
                    @Override
                    public org.qcri.rheem.basic.data.Tuple2<Long, Float> map(Tuple2<Long, Double> longDoubleTuple2) throws Exception {
                        return new org.qcri.rheem.basic.data.Tuple2<Long, Float>(longDoubleTuple2.f0, longDoubleTuple2.f1.floatValue());
                    }
                }
        );

        output.accept(dataSetOutput, flinkExecutor);

        return ExecutionOperator.modelLazyExecution(inputs, outputs, operatorContext);
    }

    @Override
    public boolean containsAction() {
        return false;
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("rheem.flink.pagerank.load.main", "rheem.flink.pagerank.load.output");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(DataSetChannel.DESCRIPTOR);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new FlinkPageRankOperator(this.numIterations);
    }
    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /**
     * A map function that assigns an initial rank to all pages.
     */
    public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
        Tuple2<Long, Double> outPageWithRank;

        public RankAssigner(double rank) {
            this.outPageWithRank = new Tuple2<Long, Double>(-1L, rank);
        }

        @Override
        public Tuple2<Long, Double> map(Long page) {
            outPageWithRank.f0 = page;
            return outPageWithRank;
        }
    }

    /**
     * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
     * originate. Run as a pre-processing step.
     */
    public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

        private final ArrayList<Long> neighbors = new ArrayList<Long>();

        @Override
        public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
            neighbors.clear();
            Long id = 0L;

            for (Tuple2<Long, Long> n : values) {
                id = n.f0;
                neighbors.add(n.f1);
            }
            out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
        }
    }

    /**
     * Join function that distributes a fraction of a vertex's rank to all neighbors.
     */
    public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

        @Override
        public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
            Long[] neighbors = value.f1.f1;
            double rank = value.f0.f1;
            double rankToDistribute = rank / ((double) neighbors.length);

            for (Long neighbor: neighbors) {
                out.collect(new Tuple2<Long, Double>(neighbor, rankToDistribute));
            }
        }
    }

    /**
     * The function that applies the page rank dampening formula.
     */
    public static final class Dampener implements MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

        private final double dampening;
        private final double randomJump;

        public Dampener(double dampening, double numVertices) {
            this.dampening = dampening;
            this.randomJump = (1 - dampening) / numVertices;
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
            value.f1 = (value.f1 * dampening) + randomJump;
            return value;
        }
    }

    /**
     * Filter that filters vertices where the rank difference is below a threshold.
     */
    public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

        @Override
        public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
            return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
        }
    }
}
