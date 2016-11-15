package org.qcri.rheem.core.platform.lineage;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.Tuple;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * A node wraps a {@link ChannelInstance} and keeps track of predecessor nodes.
 */
public abstract class LazyExecutionLineageNode {

    /**
     * Instances that need to be executed before this instance.
     */
    private final Collection<LazyExecutionLineageNode> predecessors = new LinkedList<>();

    private boolean isExecuted = false;

    /**
     * Adds a predecessor.
     *
     * @param predecessor the predecessor
     */
    public void addPredecessor(LazyExecutionLineageNode predecessor) {
        assert !this.predecessors.contains(predecessor) :
                String.format("Lineage predecessor %s is already present.", predecessor);
        this.predecessors.add(predecessor);
    }


    /**
     * Traverse this instance and all its predecessors unless they are marked as executed.
     *
     * @param accumulator state that is maintained over the traversal
     * @param aggregator  visits the traversed instances
     * @param isMark      whether traversed instances should be marked
     * @param <T>
     * @return the {@code accumulator} in its final state
     */
    public <T> T traverse(T accumulator, Aggregator<T> aggregator, boolean isMark) {
        if (!this.isExecuted) {
            for (Iterator<LazyExecutionLineageNode> i = this.predecessors.iterator(); i.hasNext(); ) {
                LazyExecutionLineageNode predecessor = i.next();
                accumulator = predecessor.traverse(accumulator, aggregator, isMark);
                if (predecessor.isExecuted) {
                    i.remove();
                }
            }
            accumulator = this.accept(accumulator, aggregator);
            if (isMark) this.markAsExecuted();
        }
        return accumulator;
    }

    protected abstract <T> T accept(T accumulator, Aggregator<T> aggregator);

    /**
     * Mark that this instance should not be traversed any more.
     */
    protected void markAsExecuted() {
        this.isExecuted = true;
    }

    public <T> T traverseAndMark(T accumulator, Aggregator<T> aggregator) {
        return this.traverse(accumulator, aggregator, true);
    }

    public <T> T traverse(T accumulator, Aggregator<T> aggregator) {
        return this.traverse(accumulator, aggregator, false);
    }

    /**
     * Set all of the {@code inputs} as predecessors of the {@code operatorContext} each of the {@code outputs}.
     *
     * @param inputs          input {@link ChannelInstance}s
     * @param operatorContext in-between {@link OptimizationContext.OperatorContext}
     * @param outputs         output {@link ChannelInstance}s
     * @see #addPredecessor(LazyExecutionLineageNode)
     */
    public static void connectAll(ChannelInstance[] inputs,
                                  OptimizationContext.OperatorContext operatorContext,
                                  ChannelInstance[] outputs) {
        for (ChannelInstance input : inputs) {
            if (input != null) operatorContext.getLineage().addPredecessor(input.getLineage());
        }
        for (ChannelInstance output : outputs) {
            if (output != null) output.getLineage().addPredecessor(operatorContext.getLineage());
        }
    }

    /**
     * Collect and mark all unmarked {@link LazyExecutionLineageNode}s in this instance.
     *
     * @return the collected {@link OptimizationContext.OperatorContext}s and produced {@link ChannelInstance}s
     */
    public Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> collectAndMark() {
        return this.collectAndMark(new LinkedList<>(), new LinkedList<>());
    }

    /**
     * Collect and mark all unmarked {@link LazyExecutionLineageNode}s in this instance.
     *
     * @param operatorContextCollector collects the {@link OptimizationContext.OperatorContext} in the unmarked {@link LazyExecutionLineageNode}s
     * @param channelInstanceCollector collects the {@link ChannelInstance} in the unmarked {@link LazyExecutionLineageNode}s
     * @return the two collectors
     */
    public Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> collectAndMark(
            Collection<OptimizationContext.OperatorContext> operatorContextCollector,
            Collection<ChannelInstance> channelInstanceCollector
    ) {
        return this.traverseAndMark(
                new Tuple<>(operatorContextCollector, channelInstanceCollector),
                new CollectingAggregator()
        );
    }

    /**
     * Callback interface for traversals of {@link LazyExecutionLineageNode}s, thereby accumulating the callback return values.
     *
     * @param <T> type of the accumulator
     */
    public interface Aggregator<T> {

        /**
         * Visit an {@link ChannelLineageNode}.
         *
         * @param accumulator     current accumulator value
         * @param channelInstance the {@link ChannelInstance} of wrapped by the visited {@link LazyExecutionLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, ChannelInstance channelInstance);

        /**
         * Visit an {@link OperatorLineageNode}.
         *
         * @param accumulator     current accumulator value
         * @param operatorContext the {@link OptimizationContext.OperatorContext} of the visited {@link LazyExecutionLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, OptimizationContext.OperatorContext operatorContext);

    }

    /**
     * {@link Aggregator} implementation that collects all visited {@link LazyExecutionLineageNode} contents.
     */
    public static class CollectingAggregator
            implements Aggregator<Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>>> {

        @Override
        public Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> aggregate(
                Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> accumulator,
                ChannelInstance channelInstance) {
            accumulator.getField1().add(channelInstance);
            return accumulator;
        }

        @Override
        public Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> aggregate(
                Tuple<Collection<OptimizationContext.OperatorContext>, Collection<ChannelInstance>> accumulator,
                OptimizationContext.OperatorContext operatorContext) {
            accumulator.getField0().add(operatorContext);
            return accumulator;
        }
    }

}
