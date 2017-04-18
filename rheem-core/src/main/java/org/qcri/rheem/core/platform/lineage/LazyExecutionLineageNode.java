package org.qcri.rheem.core.platform.lineage;

import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.LoggerFactory;

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

    /**
     * Pinned down {@link ChannelInstance}s that must not be disposed before this instance has been marked as
     * executed.
     */
    private final Collection<ChannelInstance> pinnedDownChannelInstances = new LinkedList<>();

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

        // TODO: Pinning the input ChannelInstances down like this is not very elegant.
        // A better solution would be to incorporate all LazyExecutionLineageNodes into the
        // reference counting scheme. However, this would imply considerable effort to get it right.
        if (!this.isExecuted && predecessor instanceof ChannelLineageNode) {
            ChannelInstance channelInstance = ((ChannelLineageNode) predecessor).getChannelInstance();
            this.pinnedDownChannelInstances.add(channelInstance);
            channelInstance.noteObtainedReference();
        }
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
        LoggerFactory.getLogger(this.getClass()).debug("Marking {} as executed.", this);
        this.isExecuted = true;

        // Free pinned down ChannelInstances.
        for (ChannelInstance channelInstance : this.pinnedDownChannelInstances) {
            channelInstance.noteDiscardedReference(true);
        }
        this.pinnedDownChannelInstances.clear();
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
     * @param inputs               input {@link ChannelInstance}s
     * @param executionLineageNode in-between {@link ExecutionLineageNode}
     * @param outputs              output {@link ChannelInstance}s
     * @see #addPredecessor(LazyExecutionLineageNode)
     */
    public static void connectAll(ChannelInstance[] inputs,
                                  ExecutionLineageNode executionLineageNode,
                                  ChannelInstance[] outputs) {
        for (ChannelInstance input : inputs) {
            if (input != null) executionLineageNode.addPredecessor(input.getLineage());
        }
        for (ChannelInstance output : outputs) {
            if (output != null) output.getLineage().addPredecessor(executionLineageNode);
        }
    }

    /**
     * Collect and mark all unmarked {@link ExecutionLineageNode}s in this instance.
     *
     * @return the collected {@link ExecutionLineageNode}s and produced {@link ChannelInstance}s
     */
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> collectAndMark() {
        return this.collectAndMark(new LinkedList<>(), new LinkedList<>());
    }

    /**
     * Collect and mark all unmarked {@link LazyExecutionLineageNode}s in this instance.
     *
     * @param executionLineageCollector collects the unmarked {@link ExecutionLineageNode}
     * @param channelInstanceCollector  collects the {@link ChannelInstance} in the unmarked {@link LazyExecutionLineageNode}s
     * @return the two collectors
     */
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> collectAndMark(
            Collection<ExecutionLineageNode> executionLineageCollector,
            Collection<ChannelInstance> channelInstanceCollector
    ) {
        return this.traverseAndMark(
                new Tuple<>(executionLineageCollector, channelInstanceCollector),
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
         * @param accumulator current accumulator value
         * @param node        the visited {@link ChannelLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, ChannelLineageNode node);

        /**
         * Visit an {@link ExecutionLineageNode}.
         *
         * @param accumulator current accumulator value
         * @param node        the visited {@link ExecutionLineageNode}
         * @return the new accumulator value
         */
        T aggregate(T accumulator, ExecutionLineageNode node);

    }

    /**
     * {@link Aggregator} implementation that collects all visited {@link LazyExecutionLineageNode} contents.
     */
    public static class CollectingAggregator
            implements Aggregator<Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>>> {

        @Override
        public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> aggregate(
                Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> accumulator,
                ChannelLineageNode node) {
            accumulator.getField1().add(node.getChannelInstance());
            return accumulator;
        }

        @Override
        public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> aggregate(
                Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> accumulator,
                ExecutionLineageNode node) {
            accumulator.getField0().add(node);
            return accumulator;
        }
    }

}
