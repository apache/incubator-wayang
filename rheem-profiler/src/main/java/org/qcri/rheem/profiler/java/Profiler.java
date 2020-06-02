package org.qcri.rheem.profiler.java;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.util.ProfilingUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility to support finding reasonable {@link LoadProfileEstimator}s for {@link JavaExecutionOperator}s.
 */
public class Profiler {

    private static final int GC_RUNS = 1;

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.printf("Usage: java %s <operator to profile> <cardinality>[,<cardinality>]\n", Profiler.class);
            System.exit(1);
        }

        String operator = args[0];
        List<Integer> cardinalities = Arrays.stream(args[1].split(",")).map(Integer::valueOf).collect(Collectors.toList());
        List<OperatorProfiler.Result> results;

        switch (operator) {
            case "textsource":
                results = profile(OperatorProfilers.createJavaTextFileSourceProfiler(), cardinalities);
                break;
            case "collectionsource":
                results = profile(OperatorProfilers.createJavaCollectionSourceProfiler(), cardinalities);
                break;
            case "map":
                results = profile(OperatorProfilers.createJavaMapProfiler(), cardinalities);
                break;
            case "filter":
                results = profile(OperatorProfilers.createJavaFilterProfiler(), cardinalities);
                break;
            case "flatmap":
                results = profile(OperatorProfilers.createJavaFlatMapProfiler(), cardinalities);
                break;
            case "reduce":
                results = profile(OperatorProfilers.createJavaReduceByProfiler(), cardinalities);
                break;
            case "globalreduce":
                results = profile(OperatorProfilers.createJavaGlobalReduceProfiler(), cardinalities);
                break;
            case "distinct":
            case "distinct-string":
                results = profile(OperatorProfilers.createJavaDistinctProfiler(), cardinalities);
                break;
            case "distinct-integer":
                results = profile(OperatorProfilers.createJavaDistinctProfiler(
                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                        Integer.class
                ), cardinalities);
                break;
            case "sort":
            case "sort-string":
                results = profile(OperatorProfilers.createJavaSortProfiler(), cardinalities);
                break;
            case "sort-integer":
                results = profile(OperatorProfilers.createJavaSortProfiler(
                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                        Integer.class
                ), cardinalities);
                break;
            case "count":
                results = profile(OperatorProfilers.createJavaCountProfiler(), cardinalities);
                break;
            case "groupby":
                results = profile(OperatorProfilers.createJavaMaterializedGroupByProfiler(), cardinalities);
                break;
            case "join":
                results = profile(OperatorProfilers.createJavaJoinProfiler(), cardinalities, cardinalities);
                break;
            case "union":
                results = profile(OperatorProfilers.createJavaUnionProfiler(), cardinalities, cardinalities);
                break;
            case "cartesian":
                results = profile(OperatorProfilers.createJavaCartesianProfiler(), cardinalities, cardinalities);
                break;
            case "callbacksink":
                results = profile(OperatorProfilers.createJavaLocalCallbackSinkProfiler(), cardinalities);
                break;
            case "collect":
                results = profile(OperatorProfilers.createCollectingJavaLocalCallbackSinkProfiler(), cardinalities);
                break;
            case "word-count-split": {
                final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2, 10, new Random(42));
                results = profile(
                        OperatorProfilers.createJavaFlatMapProfiler(
                                () -> String.format("%s %s %s %s %s %s %s %s %s",
                                        randomStringSupplier.get(), randomStringSupplier.get(),
                                        randomStringSupplier.get(), randomStringSupplier.get(),
                                        randomStringSupplier.get(), randomStringSupplier.get(),
                                        randomStringSupplier.get(), randomStringSupplier.get(),
                                        randomStringSupplier.get()),
                                str -> Arrays.asList(str.split(" ")),
                                String.class,
                                String.class
                        ),
                        cardinalities);
                break;
            }
            case "word-count-canonicalize": {
                final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2, 10, new Random(42));
                results = profile(
                        OperatorProfilers.createJavaMapProfiler(
                                randomStringSupplier,
                                word -> new Tuple2<>(word.toLowerCase(), 1),
                                String.class,
                                Tuple2.class
                        ),
                        cardinalities
                );
                break;
            }
            case "word-count-count": {
                final Supplier<String> stringSupplier = DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 2, 10);
                results = profile(
                        OperatorProfilers.createJavaReduceByProfiler(
                                () -> new Tuple2<>(stringSupplier.get(), 1),
                                pair -> pair.field0,
                                (p1, p2) -> {
                                    p1.field1 += p2.field1;
                                    return p1;
                                },
                                cast(Tuple2.class),
                                String.class
                        ),
                        cardinalities
                );
                break;
            }
            default:
                System.out.println("Unknown operator: " + operator);
                return;
        }

        System.out.println();
        System.out.println(RheemCollections.getAny(results).getCsvHeader());
        results.forEach(result -> System.out.println(result.toCsvString()));
    }

    private static StopWatch createStopWatch() {
        Experiment experiment = new Experiment("rheem-profiler", new Subject("Rheem", "0.1"));
        return new StopWatch(experiment);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> cast(Class<?> cls) {
        return (Class<T>) cls;
    }


    private static List<OperatorProfiler.Result> profile(SourceProfiler sourceProfiler, Collection<Integer> cardinalities) {
        return cardinalities.stream()
                .map(cardinality -> profile(sourceProfiler, cardinality))
                .collect(Collectors.toList());
    }

    private static OperatorProfiler.Result profile(SourceProfiler sourceProfiler, int cardinality) {
        System.out.println("Running garbage collector...");
        for (int i = 0; i < GC_RUNS; i++) {
            System.gc();
        }
        ProfilingUtils.sleep(1000);

        System.out.printf("Profiling %s with %d data quanta.\n", sourceProfiler, cardinality);
        final StopWatch stopWatch = createStopWatch();

        System.out.println("Prepare...");
        final TimeMeasurement preparation = stopWatch.start("Preparation");
        sourceProfiler.prepare(cardinality);
        preparation.stop();

        System.out.println("Execute...");
        final TimeMeasurement execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = sourceProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

    private static List<OperatorProfiler.Result> profile(UnaryOperatorProfiler unaryOperatorProfiler, Collection<Integer> cardinalities) {
        return cardinalities.stream()
                .map(cardinality -> profile(unaryOperatorProfiler, cardinality))
                .collect(Collectors.toList());
    }

    private static OperatorProfiler.Result profile(UnaryOperatorProfiler unaryOperatorProfiler, int cardinality) {
        System.out.println("Running garbage collector...");
        for (int i = 0; i < GC_RUNS; i++) {
            System.gc();
        }
        ProfilingUtils.sleep(1000);

        System.out.printf("Profiling %s with %d data quanta.\n", unaryOperatorProfiler, cardinality);
        final StopWatch stopWatch = createStopWatch();

        System.out.println("Prepare...");
        final TimeMeasurement preparation = stopWatch.start("Preparation");
        unaryOperatorProfiler.prepare(cardinality);
        preparation.stop();

        System.out.println("Execute...");
        final TimeMeasurement execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = unaryOperatorProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

    private static List<OperatorProfiler.Result> profile(BinaryOperatorProfiler binaryOperatorProfiler,
                                                         Collection<Integer> cardinalities0,
                                                         Collection<Integer> cardinalities1) {
        return cardinalities0.stream()
                .flatMap(cardinality0 ->
                        cardinalities1.stream()
                                .map(
                                        cardinality1 -> profile(binaryOperatorProfiler, cardinality0, cardinality1)
                                )
                )
                .collect(Collectors.toList());
    }

    private static OperatorProfiler.Result profile(BinaryOperatorProfiler binaryOperatorProfiler,
                                                   int cardinality0,
                                                   int cardinality1) {
        System.out.println("Running garbage collector...");
        for (int i = 0; i < GC_RUNS; i++) {
            System.gc();
        }
        ProfilingUtils.sleep(1000);

        System.out.printf("Profiling %s with %dx%d data quanta.\n", binaryOperatorProfiler.getOperator(), cardinality0, cardinality1);
        final StopWatch stopWatch = createStopWatch();

        System.out.println("Prepare...");
        final TimeMeasurement preparation = stopWatch.start("Preparation");
        binaryOperatorProfiler.prepare(cardinality0, cardinality1);
        preparation.stop();

        System.out.println("Execute...");
        final TimeMeasurement execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = binaryOperatorProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

    private static List<OperatorProfiler.Result> profile(SinkProfiler sinkProfiler, Collection<Integer> cardinalities) {
        return cardinalities.stream()
                .map(cardinality -> profile(sinkProfiler, cardinality))
                .collect(Collectors.toList());
    }

    private static OperatorProfiler.Result profile(SinkProfiler sinkProfiler, int cardinality) {
        System.out.println("Running garbage collector...");
        for (int i = 0; i < GC_RUNS; i++) {
            System.gc();
        }
        ProfilingUtils.sleep(1000);

        System.out.printf("Profiling %s with %d data quanta.\n", sinkProfiler, cardinality);
        final StopWatch stopWatch = createStopWatch();

        System.out.println("Prepare...");
        final TimeMeasurement preparation = stopWatch.start("Preparation");
        sinkProfiler.prepare(cardinality);
        preparation.stop();

        System.out.println("Execute...");
        final TimeMeasurement execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = sinkProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

}