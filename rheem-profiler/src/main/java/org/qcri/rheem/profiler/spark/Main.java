package org.qcri.rheem.profiler.spark;

import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.StopWatch;
import org.qcri.rheem.profiler.util.ProfilingUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Starts a profiling run of Spark.
 */
public class Main {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.printf("Usage: java %s <operator to profile> <cardinality>[,<cardinality>]\n", Main.class);
            System.exit(1);
        }

        String operator = args[0];
        List<Integer> cardinalities = Arrays.stream(args[1].split(",")).map(Integer::valueOf).collect(Collectors.toList());
        List<SparkOperatorProfiler.Result> results;

        switch (operator) {
//            case "textsource":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaTextFileSourceProfiler(), cardinalities);
//                break;
//            case "collectionsource":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaCollectionSourceProfiler(), cardinalities);
//                break;
            case "map":
                results = profile(OperatorProfilers.createSparkMapProfiler(), cardinalities);
                break;
//            case "filter":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaFilterProfiler(), cardinalities);
//                break;
//            case "flatmap":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaFlatMapProfiler(), cardinalities);
//                break;
            case "reduce":
                results = profile(OperatorProfilers.createSparkReduceByProfiler(), cardinalities);
                break;
//            case "globalreduce":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaGlobalReduceProfiler(), cardinalities);
//                break;
//            case "distinct":
//            case "distinct-string":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaDistinctProfiler(), cardinalities);
//                break;
//            case "distinct-integer":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaDistinctProfiler(
//                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
//                        Integer.class
//                ), cardinalities);
//                break;
//            case "sort":
//            case "sort-string":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaSortProfiler(), cardinalities);
//                break;
//            case "sort-integer":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaSortProfiler(
//                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
//                        Integer.class
//                ), cardinalities);
//                break;
//            case "count":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaCountProfiler(), cardinalities);
//                break;
//            case "groupby":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaMaterializedGroupByProfiler(), cardinalities);
//                break;
//            case "join":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaJoinProfiler(), cardinalities, cardinalities);
//                break;
//            case "union":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaUnionProfiler(), cardinalities, cardinalities);
//                break;
//            case "cartesian":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaCartesianProfiler(), cardinalities, cardinalities);
//                break;
//            case "callbacksink":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaLocalCallbackSinkProfiler(), cardinalities);
//                break;
//            case "collect":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createCollectingJavaLocalCallbackSinkProfiler(), cardinalities);
//                break;
//            case "word-count-split": {
//                final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2, 10, new Random(42));
//                results = profile(
//                        org.qcri.rheem.profiler.java.OperatorProfilers.createJavaFlatMapProfiler(
//                                () -> String.format("%s %s %s %s %s %s %s %s %s",
//                                        randomStringSupplier.get(), randomStringSupplier.get(),
//                                        randomStringSupplier.get(), randomStringSupplier.get(),
//                                        randomStringSupplier.get(), randomStringSupplier.get(),
//                                        randomStringSupplier.get(), randomStringSupplier.get(),
//                                        randomStringSupplier.get()),
//                                str -> Arrays.asList(str.split(" ")),
//                                String.class,
//                                String.class
//                        ),
//                        cardinalities);
//                break;
//            }
//            case "word-count-canonicalize": {
//                final Supplier<String> randomStringSupplier = DataGenerators.createRandomStringSupplier(2, 10, new Random(42));
//                results = profile(
//                        org.qcri.rheem.profiler.java.OperatorProfilers.createJavaMapProfiler(
//                                randomStringSupplier,
//                                word -> new Tuple2<>(word.toLowerCase(), 1),
//                                String.class,
//                                Tuple2.class
//                        ),
//                        cardinalities
//                );
//                break;
//            }
//            case "word-count-count": {
//                final Supplier<String> stringSupplier = DataGenerators.createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, new Random(42), 2, 10);
//                results = profile(
//                        org.qcri.rheem.profiler.java.OperatorProfilers.createJavaReduceByProfiler(
//                                () -> new Tuple2<>(stringSupplier.get(), 1),
//                                pair -> pair.field0,
//                                (p1, p2) -> {
//                                    p1.field1 += p2.field1;
//                                    return p1;
//                                },
//                                cast(Tuple2.class),
//                                String.class
//                        ),
//                        cardinalities
//                );
//                break;
//            }
            default:
                System.out.println("Unknown operator: " + operator);
                return;
        }

        System.out.println();
        System.out.println(RheemCollections.getAny(results).getCsvHeader());
        results.forEach(result -> System.out.println(result.toCsvString()));
    }

    private static List<SparkOperatorProfiler.Result> profile(UnaryOperatorProfiler unaryOperatorProfiler, Collection<Integer> cardinalities) {
        return cardinalities.stream()
                .map(cardinality -> profile(unaryOperatorProfiler, cardinality))
                .collect(Collectors.toList());
    }

    private static SparkOperatorProfiler.Result profile(UnaryOperatorProfiler unaryOperatorProfiler, int cardinality) {
        ProfilingUtils.sleep(1000);

        System.out.printf("Profiling %s with %d data quanta.\n", unaryOperatorProfiler, cardinality);
        final StopWatch stopWatch = new StopWatch();

        System.out.println("Prepare...");
        final StopWatch.Round preparation = stopWatch.start("Preparation");
        unaryOperatorProfiler.prepare(cardinality);
        preparation.stop();

        System.out.println("Execute...");
        final StopWatch.Round execution = stopWatch.start("Execution");
        final SparkOperatorProfiler.Result result = unaryOperatorProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

}
