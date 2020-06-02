package org.qcri.rheem.java.operators.graph;

import gnu.trove.iterator.TLongFloatIterator;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.TLongIntMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.map.hash.TLongIntHashMap;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.platform.lineage.ExecutionLineageNode;
import org.qcri.rheem.core.util.Tuple;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

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
        final TLongFloatMap pageRanks = this.pageRank(edges);
        final Stream<Tuple2<Long, Float>> pageRankStream = this.stream(pageRanks);

        output.accept(pageRankStream);

        return ExecutionOperator.modelQuasiEagerExecution(inputs, outputs, operatorContext);
    }

    /**
     * Execute the PageRank algorithm.
     *
     * @param edgeDataSet edges of a graph
     * @return the page ranks
     */
    private TLongFloatMap pageRank(Collection<Tuple2<Long, Long>> edgeDataSet) {
        // Get the degress of all vertices and make sure we collect *all* vertices.
        TLongIntMap degrees = new TLongIntHashMap();
        for (Tuple2<Long, Long> edge : edgeDataSet) {
            degrees.adjustOrPutValue(edge.field0, 1, 1);
            degrees.adjustOrPutValue(edge.field0, 0, 0);
        }
        int numVertices = degrees.size();
        float initialRank = 1f / numVertices;
        float dampingRank = (1 - this.dampingFactor) / numVertices;

        // Initialize the rank map.
        TLongFloatMap initialRanks = new TLongFloatHashMap();
        degrees.forEachKey(k -> {
            initialRanks.putIfAbsent(k, initialRank);
            return true;
        });

        TLongFloatMap currentRanks = initialRanks;
        for (int iteration = 0; iteration < this.getNumIterations(); iteration++) {
            // Add the damping first.
            TLongFloatMap newRanks = new TLongFloatHashMap(currentRanks.size());
            degrees.forEachKey(k -> {
                newRanks.putIfAbsent(k, dampingRank);
                return true;
            });

            // Now add the other ranks.
            for (Tuple2<Long, Long> edge : edgeDataSet) {
                final long sourceVertex = edge.field0;
                final long targetVertex = edge.field1;
                final int degree = degrees.get(sourceVertex);
                final float currentRank = currentRanks.get(sourceVertex);
                final float partialRank = this.dampingFactor * currentRank / degree;
                newRanks.adjustOrPutValue(targetVertex, partialRank, partialRank);
            }

            currentRanks = newRanks;
        }

        return currentRanks;
    }

    private Stream<Tuple2<Long, Float>> stream(TLongFloatMap map) {
        final TLongFloatIterator tLongFloatIterator = map.iterator();
        Iterator<Tuple2<Long, Float>> iterator = new Iterator<Tuple2<Long, Float>>() {
            @Override
            public boolean hasNext() {
                return tLongFloatIterator.hasNext();
            }

            @Override
            public Tuple2<Long, Float> next() {
                tLongFloatIterator.advance();
                return new Tuple2<>(tLongFloatIterator.key(), tLongFloatIterator.value());
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }


    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.java.pagerank.load";
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
