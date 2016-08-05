package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.*;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.mapping.*;
import org.qcri.rheem.core.plan.rheemplan.LoopIsolator;
import org.qcri.rheem.core.plan.rheemplan.LoopSubplan;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.plan.rheemplan.Subplan;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemCollections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This {@link Mapping} translates a {@link PageRankOperator} into a {@link Subplan} of basic {@link Operator}s.
 */
public class PageRankMapping implements Mapping {

    @Override
    public Collection<PlanTransformation> getTransformations() {
        return Collections.singleton(this.createTransformation());
    }

    private PlanTransformation createTransformation() {
        return new PlanTransformation(
                this.createPattern(),
                this.createReplacementFactory()
        );
    }

    private SubplanPattern createPattern() {
        return SubplanPattern.createSingleton(new OperatorPattern<>(
                "pageRank",
                new PageRankOperator(1),
                false
        ));
    }

    private ReplacementSubplanFactory createReplacementFactory() {
        return new ReplacementSubplanFactory.OfSingleOperators<>(this::createPageRankSubplan);
    }

    private Subplan createPageRankSubplan(PageRankOperator pageRankOperator, int epoch) {
        final String operatorBaseName = pageRankOperator.getName() == null ?
                "PageRank" :
                pageRankOperator.getName();

        // TODO: We only need this MapOperator, because we cannot have a singl Subplan InputSlot that maps to two
        // inner InputSlots.
        MapOperator<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>> forward = new MapOperator<>(
                t -> t, ReflectionUtils.specify(Tuple2.class), ReflectionUtils.specify(Tuple2.class)
        );
        forward.setName(String.format("%s (forward)", operatorBaseName));


        // Find all vertices.
        FlatMapOperator<Tuple2<Integer, Integer>, Integer> vertexExtractor = new FlatMapOperator<>(
                (FunctionDescriptor.SerializableFunction<Tuple2<Integer, Integer>, Iterable<Integer>>) integer -> {
                    final List<Integer> out = new ArrayList<>(2);
                    out.add(integer.field0);
                    out.add(integer.field1);
                    return out;
                },
                ReflectionUtils.specify(Tuple2.class), Integer.class
        );
        vertexExtractor.setName(String.format("%s (extract vertices)", operatorBaseName));
        forward.connectTo(0, vertexExtractor, 0);

        // Get the distinct vertices.
        DistinctOperator<Integer> vertexDistincter = new DistinctOperator<>(Integer.class);
        vertexDistincter.setName(String.format("%s (distinct vertices)", operatorBaseName));
        vertexExtractor.connectTo(0, vertexDistincter, 0);

        // Count the vertices.
        CountOperator<Integer> vertexCounter = new CountOperator<>(Integer.class);
        vertexCounter.setName(String.format("%s (count vertices)", operatorBaseName));
        vertexDistincter.connectTo(0, vertexCounter, 0);

        // Create the adjancencies.
        MapOperator<Tuple2<Integer, Integer>, Tuple2<Integer, int[]>> adjacencyPreparator = new MapOperator<>(
                t -> new Tuple2<>(t.field0, new int[]{t.field1}),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        adjacencyPreparator.setName(String.format("%s (prepare adjacencies)", operatorBaseName));
        forward.connectTo(0, adjacencyPreparator, 0);

        ReduceByOperator<Tuple2<Integer, int[]>, Integer> adjacencyCreator = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> {
                    // NB: We don't care about duplicates because they should influence the PageRanks.
                    // That being said, in some cases there are more efficient implementations of bags.
                    int[] targetVertices = new int[t1.field1.length + t2.field1.length];
                    System.arraycopy(t1.field1, 0, targetVertices, 0, t1.field1.length);
                    System.arraycopy(t2.field1, 0, targetVertices, t1.field1.length, t2.field1.length);
                    return new Tuple2<>(t1.field0, targetVertices);
                },
                ReflectionUtils.specify(Integer.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        adjacencyCreator.setName(String.format("%s (create adjacencies)", operatorBaseName));
        adjacencyPreparator.connectTo(0, adjacencyCreator, 0);

        // Create the initial page ranks.
        MapOperator<Integer, Tuple2<Integer, Float>> initializeRanks = new MapOperator<>(
                new FunctionDescriptor.ExtendedSerializableFunction<Integer, Tuple2<Integer, Float>>() {

                    private Float initialRank;

                    @Override
                    public void open(ExecutionContext ctx) {
                        long numVertices = RheemCollections.getSingle(ctx.getBroadcast("numVertices"));
                        this.initialRank = 1f / numVertices;
                    }

                    @Override
                    public Tuple2<Integer, Float> apply(Integer vertexId) {
                        return new Tuple2<>(vertexId, this.initialRank);
                    }
                },
                Integer.class, ReflectionUtils.specify(Tuple2.class)
        );
        initializeRanks.setName(String.format("%s (initialize ranks)", operatorBaseName));
        vertexDistincter.connectTo(0, initializeRanks, 0);
        vertexCounter.broadcastTo(0, initializeRanks, "numVertices");

        // Send the initial page ranks into the loop.
        RepeatOperator<Tuple2<Integer, int[]>> loopHead = new RepeatOperator<>(
                pageRankOperator.getNumIterations(), ReflectionUtils.specify(Tuple2.class)
        );
        loopHead.setName(String.format("%s (loop head)", operatorBaseName));
        loopHead.initialize(initializeRanks, 0);

        // Join adjacencies and current ranks.
        JoinOperator<Tuple2<Integer, int[]>, Tuple2<Integer, Float>, Integer> rankJoin =
                new JoinOperator<>(
                        Tuple2::getField0,
                        Tuple2::getField0,
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class),
                        Integer.class
                );
        rankJoin.setName(String.format("%s (join adjacencies and ranks)", operatorBaseName));
        adjacencyCreator.connectTo(0, rankJoin, 0);
        loopHead.connectTo(RepeatOperator.ITERATION_OUTPUT_INDEX, rankJoin, 1);

        // Create the new partial ranks.
        FlatMapOperator<Tuple2<Tuple2<Integer, int[]>, Tuple2<Integer, Float>>, Tuple2<Integer, Float>> partialRankCreator =
                new FlatMapOperator<>(
                        adjacencyAndRank -> {
                            Integer sourceVertex = adjacencyAndRank.field0.field0;
                            final int[] targetVertices = adjacencyAndRank.field0.field1;
                            final float baseRank = adjacencyAndRank.field1.field1;
                            final Float partialRank = baseRank / targetVertices.length;
                            Collection<Tuple2<Integer, Float>> partialRanks = new ArrayList<>(targetVertices.length + 1);
                            for (int targetVertex : targetVertices) {
                                partialRanks.add(new Tuple2<>(targetVertex, partialRank));
                            }
                            // Add a surrogate partial rank to avoid losing unreferenced vertices.
                            partialRanks.add(new Tuple2<>(sourceVertex, 0f));
                            return partialRanks;
                        },
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class)
                );
        partialRankCreator.setName(String.format("%s (create partial ranks)", operatorBaseName));
        rankJoin.connectTo(0, partialRankCreator, 0);

        // Sum the partial ranks.
        ReduceByOperator<Tuple2<Integer, Float>, Integer> sumPartialRanks = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1),
                Integer.class,
                ReflectionUtils.specify(Tuple2.class)
        );
        sumPartialRanks.setName(String.format("%s (sum partial ranks)", operatorBaseName));
        partialRankCreator.connectTo(0, sumPartialRanks, 0);

        // Apply the damping factor.
        MapOperator<Tuple2<Integer, Float>, Tuple2<Integer, Float>> damping = new MapOperator<>(
                new FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Integer, Float>, Tuple2<Integer, Float>>() {

                    private final float dampingFactor = 0.85f;

                    private float minRank;

                    @Override
                    public void open(ExecutionContext ctx) {
                        long numVertices = RheemCollections.getSingle(ctx.getBroadcast("numVertices"));
                        this.minRank = (1 - this.dampingFactor) / numVertices;
                    }

                    @Override
                    public Tuple2<Integer, Float> apply(Tuple2<Integer, Float> rank) {
                        return new Tuple2<>(
                                rank.field0,
                                this.minRank + this.dampingFactor * rank.field1
                        );
                    }
                },
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        damping.setName(String.format("%s (damping)", operatorBaseName));
        sumPartialRanks.connectTo(0, damping, 0);
        vertexCounter.broadcastTo(0, damping, "numVertices");
        loopHead.endIteration(damping, 0);

        final LoopSubplan loopSubplan = LoopIsolator.isolate(loopHead);

        return Subplan.wrap(
                Collections.singletonList(forward.getInput()),
                Collections.singletonList(loopSubplan.getOutput(0)),
                null
        );
    }
}
