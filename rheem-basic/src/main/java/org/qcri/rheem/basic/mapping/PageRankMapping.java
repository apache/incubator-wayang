package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.CountOperator;
import org.qcri.rheem.basic.operators.DistinctOperator;
import org.qcri.rheem.basic.operators.FlatMapOperator;
import org.qcri.rheem.basic.operators.JoinOperator;
import org.qcri.rheem.basic.operators.MapOperator;
import org.qcri.rheem.basic.operators.PageRankOperator;
import org.qcri.rheem.basic.operators.ReduceByOperator;
import org.qcri.rheem.basic.operators.RepeatOperator;
import org.qcri.rheem.core.function.ExecutionContext;
import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.FunctionDescriptor;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.mapping.OperatorPattern;
import org.qcri.rheem.core.mapping.PlanTransformation;
import org.qcri.rheem.core.mapping.ReplacementSubplanFactory;
import org.qcri.rheem.core.mapping.SubplanPattern;
import org.qcri.rheem.core.optimizer.ProbabilisticDoubleInterval;
import org.qcri.rheem.core.optimizer.cardinality.DefaultCardinalityEstimator;
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

    private static final double NUM_VERTICES_PER_EDGE = 0.01d;

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

    private Operator createPageRankSubplan(PageRankOperator pageRankOperator, int epoch) {
        final String operatorBaseName = pageRankOperator.getName() == null ?
                "PageRank" :
                pageRankOperator.getName();

        // TODO: We only need this MapOperator, because we cannot have a singl Subplan InputSlot that maps to two
        // inner InputSlots.
        MapOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> forward = new MapOperator<>(
                t -> t, ReflectionUtils.specify(Tuple2.class), ReflectionUtils.specify(Tuple2.class)
        );
        forward.at(epoch);
        forward.setName(String.format("%s (forward)", operatorBaseName));


        // Find all vertices.
        FlatMapOperator<Tuple2<Long, Long>, Long> vertexExtractor = new FlatMapOperator<>(
                new FlatMapDescriptor<>(
                        (FunctionDescriptor.SerializableFunction<Tuple2<Long, Long>, Iterable<Long>>) edge -> {
                            final List<Long> out = new ArrayList<>(2);
                            out.add(edge.field0);
                            out.add(edge.field1);
                            return out;
                        },
                        ReflectionUtils.specify(Tuple2.class), Long.class,
                        ProbabilisticDoubleInterval.ofExactly(2)
                )
        );
        vertexExtractor.at(epoch);
        vertexExtractor.setName(String.format("%s (extract vertices)", operatorBaseName));
        forward.connectTo(0, vertexExtractor, 0);

        // Get the distinct vertices.
        DistinctOperator<Long> vertexDistincter = new DistinctOperator<>(Long.class);
        vertexDistincter.at(epoch);
        vertexDistincter.setName(String.format("%s (distinct vertices)", operatorBaseName));
        vertexDistincter.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE / 2)
        ));
        vertexExtractor.connectTo(0, vertexDistincter, 0);

        // Count the vertices.
        CountOperator<Long> vertexCounter = new CountOperator<>(Long.class);
        vertexCounter.at(epoch);
        vertexCounter.setName(String.format("%s (count vertices)", operatorBaseName));
        vertexDistincter.connectTo(0, vertexCounter, 0);

        // Create the adjancencies.
        MapOperator<Tuple2<Long, Long>, Tuple2<Long, long[]>> adjacencyPreparator = new MapOperator<>(
                t -> new Tuple2<>(t.field0, new long[]{t.field1}),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        adjacencyPreparator.at(epoch);
        adjacencyPreparator.setName(String.format("%s (prepare adjacencies)", operatorBaseName));
        forward.connectTo(0, adjacencyPreparator, 0);

        ReduceByOperator<Tuple2<Long, long[]>, Long> adjacencyCreator = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> {
                    // NB: We don't care about duplicates because they should influence the PageRanks.
                    // That being said, in some cases there are more efficient implementations of bags.
                    long[] targetVertices = new long[t1.field1.length + t2.field1.length];
                    System.arraycopy(t1.field1, 0, targetVertices, 0, t1.field1.length);
                    System.arraycopy(t2.field1, 0, targetVertices, t1.field1.length, t2.field1.length);
                    return new Tuple2<>(t1.field0, targetVertices);
                },
                ReflectionUtils.specify(Long.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        adjacencyCreator.at(epoch);
        adjacencyCreator.setName(String.format("%s (create adjacencies)", operatorBaseName));
        adjacencyCreator.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE)
        ));
        adjacencyPreparator.connectTo(0, adjacencyCreator, 0);

        // Create the initial page ranks.
        MapOperator<Long, Tuple2<Long, Float>> initializeRanks = new MapOperator<>(
                new RankInitializer(),
                Long.class, ReflectionUtils.specify(Tuple2.class)
        );
        initializeRanks.at(epoch);
        initializeRanks.setName(String.format("%s (initialize ranks)", operatorBaseName));
        vertexDistincter.connectTo(0, initializeRanks, 0);
        vertexCounter.broadcastTo(0, initializeRanks, "numVertices");

        // Send the initial page ranks into the loop.
        RepeatOperator<Tuple2<Long, long[]>> loopHead = new RepeatOperator<>(
                pageRankOperator.getNumIterations(), ReflectionUtils.specify(Tuple2.class)
        );
        loopHead.at(epoch);
        loopHead.setName(String.format("%s (loop head)", operatorBaseName));
        loopHead.initialize(initializeRanks, 0);

        // Join adjacencies and current ranks.
        JoinOperator<Tuple2<Long, long[]>, Tuple2<Long, Float>, Long> rankJoin =
                new JoinOperator<>(
                        Tuple2::getField0,
                        Tuple2::getField0,
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class),
                        Long.class
                );
        rankJoin.at(epoch);
        rankJoin.setName(String.format("%s (join adjacencies and ranks)", operatorBaseName));
        rankJoin.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                .99d, 2, false, longs -> longs[0]
        ));
        adjacencyCreator.connectTo(0, rankJoin, 0);
        loopHead.connectTo(RepeatOperator.ITERATION_OUTPUT_INDEX, rankJoin, 1);

        // Create the new partial ranks.
        FlatMapOperator<Tuple2<Tuple2<Long, long[]>, Tuple2<Long, Float>>, Tuple2<Long, Float>> partialRankCreator =
                new FlatMapOperator<>(new FlatMapDescriptor<>(
                        adjacencyAndRank -> {
                            Long sourceVertex = adjacencyAndRank.field0.field0;
                            final long[] targetVertices = adjacencyAndRank.field0.field1;
                            final float baseRank = adjacencyAndRank.field1.field1;
                            final Float partialRank = baseRank / targetVertices.length;
                            Collection<Tuple2<Long, Float>> partialRanks = new ArrayList<>(targetVertices.length + 1);
                            for (long targetVertex : targetVertices) {
                                partialRanks.add(new Tuple2<>(targetVertex, partialRank));
                            }
                            // Add a surrogate partial rank to avoid losing unreferenced vertices.
                            partialRanks.add(new Tuple2<>(sourceVertex, 0f));
                            return partialRanks;
                        },
                        ReflectionUtils.specify(Tuple2.class),
                        ReflectionUtils.specify(Tuple2.class),
                        ProbabilisticDoubleInterval.ofExactly(1d / NUM_VERTICES_PER_EDGE)
                ));
        partialRankCreator.at(epoch);
        partialRankCreator.setName(String.format("%s (create partial ranks)", operatorBaseName));
        rankJoin.connectTo(0, partialRankCreator, 0);

        // Sum the partial ranks.
        ReduceByOperator<Tuple2<Long, Float>, Long> sumPartialRanks = new ReduceByOperator<>(
                Tuple2::getField0,
                (t1, t2) -> new Tuple2<>(t1.field0, t1.field1 + t2.field1),
                Long.class,
                ReflectionUtils.specify(Tuple2.class)
        );
        sumPartialRanks.at(epoch);
        sumPartialRanks.setName(String.format("%s (sum partial ranks)", operatorBaseName));
        sumPartialRanks.setCardinalityEstimator(0, new DefaultCardinalityEstimator(
                0.5d, 1, false, longs -> Math.round(longs[0] * NUM_VERTICES_PER_EDGE)
        ));
        partialRankCreator.connectTo(0, sumPartialRanks, 0);

        // Apply the damping factor.
        MapOperator<Tuple2<Long, Float>, Tuple2<Long, Float>> damping = new MapOperator<>(
                new ApplyDamping(pageRankOperator.getDampingFactor()),
                ReflectionUtils.specify(Tuple2.class),
                ReflectionUtils.specify(Tuple2.class)
        );
        damping.at(epoch);
        damping.setName(String.format("%s (damping)", operatorBaseName));
        sumPartialRanks.connectTo(0, damping, 0);
        vertexCounter.broadcastTo(0, damping, "numVertices");
        loopHead.endIteration(damping, 0);

        final LoopSubplan loopSubplan = LoopIsolator.isolate(loopHead);
        loopSubplan.at(epoch);

        return Subplan.wrap(
                Collections.singletonList(forward.getInput()),
                Collections.singletonList(loopSubplan.getOutput(0)),
                null
        ).at(epoch);
    }

    /**
     * Creates intial page ranks.
     */
    public static class RankInitializer
            implements FunctionDescriptor.ExtendedSerializableFunction<Long, Tuple2<Long, Float>> {

        private Float initialRank;

        @Override
        public void open(ExecutionContext ctx) {
            long numVertices = RheemCollections.getSingle(ctx.getBroadcast("numVertices"));
            this.initialRank = 1f / numVertices;
        }

        @Override
        public Tuple2<Long, Float> apply(Long vertexId) {
            return new Tuple2<>(vertexId, this.initialRank);
        }
    }

    /**
     * Applies damping to page ranks.
     */
    private static class ApplyDamping implements
            FunctionDescriptor.ExtendedSerializableFunction<Tuple2<Long, Float>, Tuple2<Long, Float>> {

        private final float dampingFactor;

        private float minRank;

        private ApplyDamping(float dampingFactor) {
            this.dampingFactor = dampingFactor;
        }

        @Override
        public void open(ExecutionContext ctx) {
            long numVertices = RheemCollections.getSingle(ctx.getBroadcast("numVertices"));
            this.minRank = (1 - this.dampingFactor) / numVertices;
        }

        @Override
        public Tuple2<Long, Float> apply(Tuple2<Long, Float> rank) {
            return new Tuple2<>(
                    rank.field0,
                    this.minRank + this.dampingFactor * rank.field1
            );
        }
    }
}
