package org.qcri.rheem.java.operators.graph;

import gnu.trove.iterator.TIntFloatIterator;
import gnu.trove.map.TIntFloatMap;
import gnu.trove.map.TIntIntMap;
import gnu.trove.map.hash.TIntFloatHashMap;
import gnu.trove.map.hash.TIntIntHashMap;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.CollectionChannel;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.compiler.FunctionCompiler;
import org.qcri.rheem.java.operators.JavaExecutionOperator;

import java.util.*;
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
    public void evaluate(ChannelInstance[] inputs, ChannelInstance[] outputs, FunctionCompiler compiler) {
        CollectionChannel.Instance input = (CollectionChannel.Instance) inputs[0];
        StreamChannel.Instance output = (StreamChannel.Instance) outputs[0];

        final Collection<Tuple2<Integer, Integer>> edges = input.provideCollection();
        final TIntFloatMap pageRanks = this.pageRank(edges);
        final Stream<Tuple2<Integer, Float>> pageRankStream = this.stream(pageRanks);

        output.accept(pageRankStream);
    }

    /**
     * Execute the PageRank algorithm.
     *
     * @param edgeDataSet edges of a graph
     * @return the page ranks
     */
    private TIntFloatMap pageRank(Collection<Tuple2<Integer, Integer>> edgeDataSet) {
        // Get the degress of all vertices and make sure we collect *all* vertices.
        TIntIntMap degrees = new TIntIntHashMap();
        for (Tuple2<Integer, Integer> edge : edgeDataSet) {
            degrees.adjustOrPutValue(edge.field0, 1, 1);
            degrees.adjustOrPutValue(edge.field0, 0, 0);
        }
        int numVertices = degrees.size();
        float initialRank = 1f / numVertices;
        float dampingRank = (1 - 0.85f) / numVertices;

        // Initialize the rank map.
        TIntFloatMap initialRanks = new TIntFloatHashMap();
        degrees.forEachKey(k -> {
            initialRanks.putIfAbsent(k, initialRank);
            return true;
        });

        TIntFloatMap currentRanks = initialRanks;
        for (int iteration = 0; iteration < this.getNumIterations(); iteration++) {
            // Add the damping first.
            TIntFloatMap newRanks = new TIntFloatHashMap(currentRanks.size());
            degrees.forEachKey(k -> {
                newRanks.putIfAbsent(k, dampingRank);
                return true;
            });

            // Now add the other ranks.
            for (Tuple2<Integer, Integer> edge : edgeDataSet) {
                final int sourceVertex = edge.field0;
                final int targetVertex = edge.field1;
                final int degree = degrees.get(sourceVertex);
                final float currentRank = currentRanks.get(sourceVertex);
                final float partialRank = .85f * currentRank / degree;
                newRanks.adjustOrPutValue(targetVertex, partialRank, partialRank);
            }

            currentRanks = newRanks;
        }

        return currentRanks;
    }

    private Stream<Tuple2<Integer, Float>> stream(TIntFloatMap map) {
        final TIntFloatIterator tIntFloatIterator = map.iterator();
        Iterator<Tuple2<Integer, Float>> iterator = new Iterator<Tuple2<Integer, Float>>() {
            @Override
            public boolean hasNext() {
                return tIntFloatIterator.hasNext();
            }

            @Override
            public Tuple2<Integer, Float> next() {
                tIntFloatIterator.advance();
                return new Tuple2<>(tIntFloatIterator.key(), tIntFloatIterator.value());
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
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
