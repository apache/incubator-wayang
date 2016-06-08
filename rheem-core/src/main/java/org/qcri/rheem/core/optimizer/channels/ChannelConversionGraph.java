package org.qcri.rheem.core.optimizer.channels;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.optimizer.OptimizationUtils;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.optimizer.costs.TimeEstimate;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.plan.rheemplan.InputSlot;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;
import org.qcri.rheem.core.platform.ChannelDescriptor;
import org.qcri.rheem.core.platform.Junction;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.util.OneTimeExecutable;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * This graph contains a set of {@link ChannelConversion}s.
 */
public class ChannelConversionGraph {

    private final Configuration configuration;

    private final Map<ChannelDescriptor, List<ChannelConversion>> conversions = new HashMap<>();

    private final Comparator<TimeEstimate> timeEstimateComparator;

    private static final BitSet EMPTY_BITSET = new BitSet(0);

    private static final Logger logger = LoggerFactory.getLogger(ChannelConversionGraph.class);

    public ChannelConversionGraph(Configuration configuration) {
        this.configuration = configuration;
        this.timeEstimateComparator = configuration.getTimeEstimateComparatorProvider().provide();
        for (final Platform platform : configuration.getPlatformProvider()) {
            platform.addChannelConversionsTo(this);
        }
    }

    /**
     * Register a new {@code channelConversion} in this instance, which effectively adds an edge.
     */
    public void add(ChannelConversion channelConversion) {
        final List<ChannelConversion> edges = this.getOrCreateChannelConversions(channelConversion.getSourceChannelDescriptor());
        edges.add(channelConversion);
    }

    /**
     * Return all registered {@link ChannelConversion}s that convert the given {@code channelDescriptor}.
     *
     * @param channelDescriptor should be converted
     * @return the {@link ChannelConversion}s
     */
    private List<ChannelConversion> getOrCreateChannelConversions(ChannelDescriptor channelDescriptor) {
        return this.conversions.computeIfAbsent(channelDescriptor, key -> new ArrayList<>());
    }

    /**
     * Finds the minimum tree {@link Junction} (w.r.t. {@link TimeEstimate}s that connects the given {@link OutputSlot} to the
     * {@code destInputSlots}.
     *
     * @param output              {@link OutputSlot} of an {@link ExecutionOperator} that should be consumed
     * @param destInputSlots      {@link InputSlot}s of {@link ExecutionOperator}s that should receive data from the {@code output}
     * @param optimizationContext describes the above mentioned {@link ExecutionOperator} key figures   @return a {@link Junction} or {@code null} if none could be found
     */
    public Junction findMinimumCostJunction(OutputSlot<?> output,
                                            List<InputSlot<?>> destInputSlots,
                                            OptimizationContext optimizationContext) {
        return this.findMinimumCostJunction(output, null, destInputSlots, optimizationContext);
    }

    /**
     * Finds the minimum tree {@link Junction} (w.r.t. {@link TimeEstimate}s that connects the given {@link OutputSlot} to the
     * {@code destInputSlots}.
     *
     * @param output              {@link OutputSlot} of an {@link ExecutionOperator} that should be consumed
     * @param existingChannel     an existing {@link Channel} that must be part of the tree or {@code null}
     * @param destInputSlots      {@link InputSlot}s of {@link ExecutionOperator}s that should receive data from the {@code output}
     * @param optimizationContext describes the above mentioned {@link ExecutionOperator} key figures   @return a {@link Junction} or {@code null} if none could be found
     */
    public Junction findMinimumCostJunction(OutputSlot<?> output,
                                            Channel existingChannel,
                                            List<InputSlot<?>> destInputSlots,
                                            OptimizationContext optimizationContext) {
        return new ShortestTreeSearcher(output, existingChannel, destInputSlots, optimizationContext, this.configuration).getJunction();
    }

    /**
     * Given two {@link Tree}s, choose the one with lower costs.
     */
    private Tree selectCheaperTree(Tree t1, Tree t2) {
        return this.timeEstimateComparator.compare(t1.costs, t2.costs) <= 0 ? t1 : t2;
    }

    /**
     * Merge several {@link Tree}s, which should have the same root but should be otherwise disjoint.
     *
     * @return the merged {@link Tree} or {@code null} if the input {@code trees} could not be merged
     */
    private Tree mergeTrees(Collection<Tree> trees) {
        assert trees.size() >= 2;

        // For various trees to be combined, we require them to be "disjoint". Check this.
        // TODO: This might be a little bit too strict.
        final Iterator<Tree> iterator = trees.iterator();
        final Tree firstTree = iterator.next();
        BitSet combinationSettledIndices = copy(firstTree.settledDestinationIndices);
        int maxSettledIndices = combinationSettledIndices.cardinality();
        final HashSet<ChannelDescriptor> employedChannelDescriptors = new HashSet<>(firstTree.employedChannelDescriptors);
        int maxVisitedChannelDescriptors = employedChannelDescriptors.size();
        TimeEstimate costs = firstTree.costs;
        TreeVertex newRoot = new TreeVertex(firstTree.root.channelDescriptor, firstTree.root.settledIndices);
        newRoot.copyEdgesFrom(firstTree.root);

        while (iterator.hasNext()) {
            final Tree ithTree = iterator.next();

            combinationSettledIndices.or(ithTree.settledDestinationIndices);
            maxSettledIndices += ithTree.settledDestinationIndices.cardinality();
            if (maxSettledIndices > combinationSettledIndices.cardinality()) {
                return null;
            }
            employedChannelDescriptors.addAll(ithTree.employedChannelDescriptors);
            maxVisitedChannelDescriptors += ithTree.employedChannelDescriptors.size() - 1; // NB: -1 for the root
            if (maxVisitedChannelDescriptors > employedChannelDescriptors.size()) {
                return null;
            }

            costs = costs.plus(ithTree.costs);
            newRoot.copyEdgesFrom(ithTree.root);
        }

        // If all tests are passed, create the combination.
        final Tree mergedTree = new Tree(newRoot, combinationSettledIndices);
        mergedTree.costs = costs;
        mergedTree.employedChannelDescriptors.addAll(employedChannelDescriptors);
        return mergedTree;
    }

    /**
     * Creates a copy of the given {@code bitSet}.
     */
    private static BitSet copy(BitSet bitSet) {
        return (BitSet) bitSet.clone();
    }


    /**
     * Finds the shortest tree between the {@link #startChannelDescriptor} and the {@link #destChannelDescriptorSets}.
     */
    private class ShortestTreeSearcher extends OneTimeExecutable {

        private final OutputSlot<?> sourceOutput;

        private final CardinalityEstimate cardinality;

        private final int numExecutions;

        private final ChannelDescriptor startChannelDescriptor;

        private final Channel sourceChannel, startChannel;

        private final Collection<ChannelDescriptor> previsitedChannels;

        private final List<InputSlot<?>> destInputs;

        private final List<Set<ChannelDescriptor>> destChannelDescriptorSets;

        private final OptimizationContext optimizationContext;

        private final Configuration configuration;

        private Map<Set<ChannelDescriptor>, BitSet> kernelDestChannelDescriptorSetsToIndices;

        private Map<ChannelDescriptor, BitSet> kernelDestChannelDescriptorsToIndices;

        private Map<ChannelConversion, TimeEstimate> conversionTimeCache = new HashMap<>();

        private Junction result = null;

        private ShortestTreeSearcher(OutputSlot<?> sourceOutput,
                                     Channel existingChannel,
                                     List<InputSlot<?>> destInputs,
                                     OptimizationContext optimizationContext,
                                     Configuration configuration) {
            this.sourceOutput = sourceOutput;
            final ExecutionOperator outputOperator = (ExecutionOperator) this.sourceOutput.getOwner();
            final OptimizationContext.OperatorContext operatorContext = optimizationContext.getOperatorContext(outputOperator);
            assert operatorContext != null : String.format("Optimization info for %s missing.", outputOperator);
            this.cardinality = operatorContext.getOutputCardinality(this.sourceOutput.getIndex());
            this.numExecutions = operatorContext.getNumExecutions();
            if (existingChannel != null) {
                Channel allegedSourceChannel = existingChannel;
                this.previsitedChannels = new ArrayList<>(4);
                while (allegedSourceChannel.getProducer().getOperator() != outputOperator) {
                    allegedSourceChannel = OptimizationUtils.getPredecessorChannel(allegedSourceChannel);
                    this.previsitedChannels.add(allegedSourceChannel.getDescriptor());
                }
                this.sourceChannel = allegedSourceChannel;
                this.startChannel = existingChannel.copy();
                this.startChannelDescriptor = this.startChannel.getDescriptor();
            } else {
                this.startChannelDescriptor = outputOperator.getOutputChannelDescriptor(this.sourceOutput.getIndex());
                this.sourceChannel = null;
                this.previsitedChannels = Collections.emptyList();
                this.startChannel = null;
            }
            this.destInputs = destInputs;
            this.destChannelDescriptorSets = RheemCollections.map(destInputs, this::resolveSupportedChannels);
            assert this.destChannelDescriptorSets.stream().noneMatch(Collection::isEmpty);
            this.optimizationContext = new OptimizationContext(optimizationContext);
            this.configuration = configuration;
        }

        public Junction getJunction() {
            this.tryExecute();
            return this.result;
        }

        /**
         * Find the supported {@link ChannelDescriptor}s for the given {@link InputSlot}. If the latter is a
         * "loop invariant" {@link InputSlot}, then require to only reusable {@link ChannelDescriptor}.
         *
         * @param input for which supported {@link ChannelDescriptor}s are requested
         * @return all eligible {@link ChannelDescriptor}s
         */
        private Set<ChannelDescriptor> resolveSupportedChannels(final InputSlot<?> input) {
            final ExecutionOperator owner = (ExecutionOperator) input.getOwner();
            final List<ChannelDescriptor> supportedInputChannels = owner.getSupportedInputChannels(input.getIndex());
            if (input.isLoopInvariant()) {
                // Loop input is needed in several iterations and must therefore be reusable.
                return supportedInputChannels.stream().filter(ChannelDescriptor::isReusable).collect(Collectors.toSet());
            } else {
                return RheemCollections.asSet(supportedInputChannels);
            }
        }

        @Override
        protected void doExecute() {
            // Make the search problem easier by condensing the search query.
            this.kernelizeChannelRequests();

            // Start from the root vertex.
            final Tree tree = this.searchTree();
            if (tree != null) {
                this.createJunction(tree);
            } else {
                logger.debug("Could not connect {} with {}.", this.sourceOutput, this.destInputs);
            }
        }

        /**
         * Rule out any non-reusable {@link ChannelDescriptor}s in recurring {@link ChannelDescriptor} sets.
         *
         * @see #kernelDestChannelDescriptorSetsToIndices
         * @see #kernelDestChannelDescriptorsToIndices
         */
        private void kernelizeChannelRequests() {
            // Check if the Junction enters a look.
            final LoopSubplan outputLoop = this.sourceOutput.getOwner().getInnermostLoop();
            final int outputLoopDepth = this.sourceOutput.getOwner().getLoopStack().size();
            boolean isSideEnterLoop = this.destInputs.stream().anyMatch(input ->
                    !input.getOwner().isLoopHead() &&
                            (input.getOwner().getLoopStack().size() > outputLoopDepth ||
                                    (input.getOwner().getLoopStack().size() == outputLoopDepth && input.getOwner().getInnermostLoop() != outputLoop)
                            )
            );

            // Merge equal Channel requests.
            this.kernelDestChannelDescriptorSetsToIndices = new HashMap<>(this.destChannelDescriptorSets.size());
            int index = 0;
            for (Set<ChannelDescriptor> destChannelDescriptorSet : this.destChannelDescriptorSets) {
                final BitSet indices = this.kernelDestChannelDescriptorSetsToIndices.computeIfAbsent(
                        destChannelDescriptorSet, key -> new BitSet(this.destChannelDescriptorSets.size())
                );
                indices.set(index++);
            }

            // Strip off the non-reusable, superfluous ChannelDescriptors.
            Collection<Tuple<Set<ChannelDescriptor>, BitSet>> channelsToIndicesChanges = new LinkedList<>();
            final Iterator<Map.Entry<Set<ChannelDescriptor>, BitSet>> iterator = this.kernelDestChannelDescriptorSetsToIndices.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Set<ChannelDescriptor>, BitSet> entry = iterator.next();
                final BitSet indices = entry.getValue();
                if (indices.cardinality() < 2 && !isSideEnterLoop) continue;

                Set<ChannelDescriptor> channelDescriptors = entry.getKey();
                int numReusableChannels = (int) channelDescriptors.stream().filter(ChannelDescriptor::isReusable).count();
                if (numReusableChannels == 0 || numReusableChannels == channelDescriptors.size()) continue;

                iterator.remove();
                channelDescriptors = new HashSet<>(channelDescriptors);
                channelDescriptors.removeIf(channelDescriptor -> !channelDescriptor.isReusable());
                channelsToIndicesChanges.add(new Tuple<>(channelDescriptors, indices));
            }
            for (Tuple<Set<ChannelDescriptor>, BitSet> channelsToIndicesChange : channelsToIndicesChanges) {
                this.kernelDestChannelDescriptorSetsToIndices.computeIfAbsent(
                        channelsToIndicesChange.getField0(),
                        key -> new BitSet(this.destChannelDescriptorSets.size())
                ).or(channelsToIndicesChange.getField1());
            }

            // Index the requested
            this.kernelDestChannelDescriptorsToIndices = new HashMap<>();
            for (Map.Entry<Set<ChannelDescriptor>, BitSet> entry : this.kernelDestChannelDescriptorSetsToIndices.entrySet()) {
                final Set<ChannelDescriptor> channelDescriptorSet = entry.getKey();
                final BitSet indices = entry.getValue();

                for (ChannelDescriptor channelDescriptor : channelDescriptorSet) {
                    this.kernelDestChannelDescriptorsToIndices.merge(channelDescriptor, indices, this::union);
                }
            }
        }

        private BitSet union(BitSet i1, BitSet i2) {
            i1.or(i2);
            return i1;
        }

        /**
         * Starts the actual search.
         */
        private Tree searchTree() {
            final HashSet<ChannelDescriptor> visitedChannelDescriptors = new HashSet<>(this.previsitedChannels);
            visitedChannelDescriptors.add(this.startChannelDescriptor);
            final Map<BitSet, Tree> solutions = this.enumerate(visitedChannelDescriptors, this.startChannelDescriptor, EMPTY_BITSET);
            BitSet requestedIndices = new BitSet(this.destChannelDescriptorSets.size());
            requestedIndices.flip(0, this.destChannelDescriptorSets.size());
            return solutions.get(requestedIndices);
        }

        /**
         * Recursive {@link Tree} enumeration strategy.
         *
         * @param visitedChannelDescriptors previously visited {@link ChannelDescriptor}s (inclusive of {@code channelDescriptor};
         *                                  can be altered but must be in original state before leaving the method
         * @param channelDescriptor         the currently enumerated {@link ChannelDescriptor}
         * @param settledDestinationIndices indices of destinations that have already been reached via the
         *                                  {@code visitedChannelDescriptors} (w/o {@code channelDescriptor};
         *                                  can be altered but must be in original state before leaving the method
         * @return solutions to the search problem reachable from this node; {@link Tree}s must still be rerooted
         */
        public Map<BitSet, Tree> enumerate(
                Set<ChannelDescriptor> visitedChannelDescriptors,
                ChannelDescriptor channelDescriptor,
                BitSet settledDestinationIndices) {

            // Mapping from settled indices to the cheapest tree settling them. Will be the return value.
            Map<BitSet, Tree> newSolutions = new HashMap<>();
            Tree newSolution;

            // Check if current path is a (new) solution.
            // TODO: Check whether the channelDescriptor is reusable.
            final BitSet newSettledIndices =
                    (BitSet) this.kernelDestChannelDescriptorsToIndices.getOrDefault(channelDescriptor, EMPTY_BITSET).clone();
            newSettledIndices.andNot(settledDestinationIndices);
            if (!newSettledIndices.isEmpty()) {
                // Create a new solution.
                settledDestinationIndices.or(newSettledIndices);
                newSolution = new Tree(new TreeVertex(channelDescriptor, newSettledIndices), copy(settledDestinationIndices));
                settledDestinationIndices.andNot(newSettledIndices);
                newSolutions.put(newSolution.settledDestinationIndices, newSolution);

                // Check if all destinations are settled.
                if (newSolution.settledDestinationIndices.cardinality() == this.destChannelDescriptorSets.size()) {
                    return newSolutions;
                }
            }

            // For each outgoing edge, explore all combinations of reachable target indices.
            if (channelDescriptor.isReusable()) {
                // When descending, "pick" the newly settled destinations only for reusable ChannelDescriptors.
                settledDestinationIndices.or(newSettledIndices);
            }
            final List<ChannelConversion> channelConversions =
                    ChannelConversionGraph.this.conversions.getOrDefault(channelDescriptor, Collections.emptyList());
            final List<Map<BitSet, Tree>> childSolutionSets = new ArrayList<>(channelConversions.size());
            for (ChannelConversion channelConversion : channelConversions) {
                final ChannelDescriptor targetChannelDescriptor = channelConversion.getTargetChannelDescriptor();
                if (visitedChannelDescriptors.add(targetChannelDescriptor)) {
                    final Map<BitSet, Tree> childSolutions = this.enumerate(
                            visitedChannelDescriptors,
                            targetChannelDescriptor,
                            settledDestinationIndices
                    );
                    childSolutions.values().forEach(
                            tree -> tree.reroot(
                                    channelDescriptor,
                                    newSettledIndices,
                                    channelConversion,
                                    this.getTimeEstimate(channelConversion)
                            )
                    );
                    childSolutionSets.add(childSolutions);

                    visitedChannelDescriptors.remove(targetChannelDescriptor);
                }
            }
            settledDestinationIndices.andNot(newSettledIndices);

            // Merge the childSolutionSets into the newSolutions.
            // Each childSolutionSet corresponds to a traversed outgoint ChannelConversion.

            // At first, consider the childSolutionSet for each outgoing ChannelConversion individually.
            for (Map<BitSet, Tree> childSolutionSet : childSolutionSets) {
                // Each childSolutionSet its has a mapping from settled indices to trees.
                for (Map.Entry<BitSet, Tree> bitSetTreeEntry : childSolutionSet.entrySet()) {
                    // Update newSolutions if the current tree is cheaper or settling new indices.
                    final BitSet childSolutionSettledIndices = bitSetTreeEntry.getKey();
                    final Tree tree = bitSetTreeEntry.getValue();
                    newSolutions.merge(childSolutionSettledIndices, tree, ChannelConversionGraph.this::selectCheaperTree);
                }
            }


            // If the current Channel/vertex is reusable, also detect valid combinations.
            // Check if the combinations yield new solutions.
            if (channelDescriptor.isReusable() && childSolutionSets.size() > 1) {
                // TODO: We could do a better job here if we would consider the "kernelized" destinations.
                int numUnreachedDestionations = this.destChannelDescriptorSets.size() - newSettledIndices.cardinality() - settledDestinationIndices.cardinality();
                if (numUnreachedDestionations >= 2) {
                    final Collection<List<Map<BitSet, Tree>>> childSolutionSetCombinations = RheemCollections.createPowerList(childSolutionSets, numUnreachedDestionations);
                    childSolutionSetCombinations.removeIf(e -> e.size() < 2);
                    for (List<Tree> solutionCombination : RheemCollections.streamedCrossProduct(RheemCollections.map(childSolutionSets, Map::values))) {
                        final Tree tree = ChannelConversionGraph.this.mergeTrees(solutionCombination);
                        if (tree != null) {
                            newSolutions.merge(tree.settledDestinationIndices, tree, ChannelConversionGraph.this::selectCheaperTree);
                        }
                    }
                }
            }

            return newSolutions;
        }

        private TimeEstimate getTimeEstimate(ChannelConversion channelConversion) {
            return this.conversionTimeCache.computeIfAbsent(
                    channelConversion,
                    key -> key.estimateConversionTime(this.cardinality, this.numExecutions, this.configuration)
            );
        }

        private void createJunction(Tree tree) {
            // Create the a new Junction.
            final Junction junction = new Junction(this.sourceOutput, this.destInputs, this.optimizationContext);

            // Create the Channels and ExecutionTasks.
            Channel sourceChannel = this.sourceChannel == null ?
                    this.startChannelDescriptor.createChannel(this.sourceOutput, this.configuration) :
                    this.sourceChannel;
            junction.setSourceChannel(sourceChannel);
            Channel startChannel = this.startChannel == null ? sourceChannel : this.startChannel;
            this.createJunctionAux(tree.root, startChannel, junction, true);

            // Assign appropriate LoopSubplans to the newly created ExecutionTasks.
            //
            // Determine the LoopSubplan from the "source side" of the Junction.
            final OutputSlot<?> sourceOutput = sourceChannel.getProducerSlot();
            final ExecutionOperator sourceOperator = (ExecutionOperator) sourceOutput.getOwner();
            final LoopSubplan sourceLoop =
                    (!sourceOperator.isLoopHead() || sourceOperator.isFeedforwardOutput(sourceOutput)) ?
                            sourceOperator.getInnermostLoop() : null;

            if (sourceLoop != null) {
                // If the source side is determining a LoopSubplan, it should be what the "target sides" request.
                for (int destIndex = 0; destIndex < this.destInputs.size(); destIndex++) {
                    assert this.destInputs.get(destIndex).getOwner().getInnermostLoop() == sourceLoop :
                            String.format(
                                    "Expected that %s would belong to %s, just as %d.",
                                    this.destInputs.get(destIndex), sourceLoop, sourceOutput
                            );
                    Channel targetChannel = junction.getTargetChannel(destIndex);
                    while (targetChannel != sourceChannel) {
                        final ExecutionTask producer = targetChannel.getProducer();
                        producer.getOperator().setContainer(sourceLoop);
                        assert producer.getNumInputChannels() == 1 : String.format(
                                "Glue operator %s was expected to have exactly one input channel.",
                                producer
                        );
                        targetChannel = producer.getInputChannel(0);
                    }
                }

            } else {
                // TODO:
                // Try to find for each distinct target LoopSubplan a "stallable" Channel, thereby ensuring not to
                // assign ExecutionTasks twice.
//                TIntCollection looplessDests = new TIntLinkedList();
//                Map<LoopSubplan, TIntCollection> loopDests = new HashMap<>();
//                for (int destIndex = 0; destIndex < this.destInputs.size(); destIndex++) {
//                    final LoopSubplan targetLoop = this.destInputs.get(destIndex).getOwner().getInnermostLoop();
//                    Channel targetChannel = junction.getTargetChannel(destIndex);
//                    if (targetLoop == null) {
//                        looplessDests.add(destIndex);
//                    } else {
//                        loopDests.computeIfAbsent(targetLoop, key -> new TIntLinkedList()).add(destIndex);
//                    }
//                }
            }

            this.result = junction;
        }

        private void createJunctionAux(TreeVertex vertex, Channel channel, Junction junction, boolean isOnAllPaths) {
            channel.setBreakingProhibited(!isOnAllPaths);

            // A bit of a hacky detail: declared settled indices of the channel are only valid if the channel can be
            // reused or if it is "terminal". Otherwise, the search algorithm will have neglected that settled index.
            if (channel.isReusable() || vertex.outEdges.isEmpty()) {
                for (int index = vertex.settledIndices.nextSetBit(0); index >= 0; index = vertex.settledIndices.nextSetBit(index + 1)) {
                    junction.setTargetChannel(index, channel);
                }
            }
            isOnAllPaths &= vertex.settledIndices.isEmpty() && vertex.outEdges.size() <= 1;
            for (TreeEdge edge : vertex.outEdges) {
                final ChannelConversion channelConversion = edge.channelConversion;
                final Channel targetChannel = channelConversion.convert(channel, this.configuration);
                if (targetChannel != channel) {
                    final ExecutionOperator conversionOperator = targetChannel.getProducer().getOperator();
                    conversionOperator.setName(String.format(
                            "convert %s", junction.getSourceOutput()
                    ));
                    junction.register(conversionOperator, this.getTimeEstimate(channelConversion));
                }
                this.createJunctionAux(edge.destination, targetChannel, junction, isOnAllPaths);
            }
        }


    }

    /**
     * A tree consisting of {@link TreeVertex}es connected by {@link TreeEdge}s.
     */
    private class Tree {

        private TreeVertex root;

        private final BitSet settledDestinationIndices;

        private final Set<ChannelDescriptor> employedChannelDescriptors = new HashSet<>();

        private TimeEstimate costs = TimeEstimate.ZERO;

        Tree(TreeVertex root, BitSet settledDestinationIndices) {
            this.root = root;
            this.settledDestinationIndices = settledDestinationIndices;
            this.employedChannelDescriptors.add(root.channelDescriptor);
        }

        void reroot(ChannelDescriptor newRootChannelDescriptor,
                    BitSet settledIndices,
                    ChannelConversion newToObsoleteRootConversion,
                    TimeEstimate costs) {
            this.employedChannelDescriptors.add(newRootChannelDescriptor);
            final TreeVertex newRoot = new TreeVertex(newRootChannelDescriptor, settledIndices);
            newRoot.linkTo(newToObsoleteRootConversion, this.root);
            this.root = newRoot;
            this.costs = this.costs.plus(costs);
        }
    }

    /**
     * Vertex in a {@link Tree}.
     */
    private static class TreeVertex {

        private final ChannelDescriptor channelDescriptor;

        private final List<TreeEdge> outEdges;

        private final BitSet settledIndices;

        private TreeVertex(ChannelDescriptor channelDescriptor, BitSet settledIndices) {
            this.channelDescriptor = channelDescriptor;
            this.settledIndices = settledIndices;
            this.outEdges = new ArrayList<>(4);
        }

        private void linkTo(ChannelConversion channelConversion, TreeVertex destination) {
            this.outEdges.add(new TreeEdge(channelConversion, destination));
        }

        private void copyEdgesFrom(TreeVertex that) {
            assert this.channelDescriptor.equals(that.channelDescriptor);
            this.outEdges.addAll(that.outEdges);
        }

    }

    /**
     * Edge in a {@link Tree}.
     */
    private static class TreeEdge {

        private final TreeVertex destination;

        private final ChannelConversion channelConversion;

        private TreeEdge(ChannelConversion channelConversion, TreeVertex destination) {
            this.channelConversion = channelConversion;
            this.destination = destination;
        }
    }
}
