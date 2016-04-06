package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.StopWatch;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.util.ProfilingUtils;
import org.qcri.rheem.profiler.util.RrdAccessor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.rrd4j.ConsolFun;

import java.util.*;
import java.util.function.Supplier;
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
//            case "reduce":
//                results = profile(org.qcri.rheem.profiler.java.OperatorProfilers.createJavaReduceByProfiler(), cardinalities);
//                break;
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

    public static void main2(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java ... <RRD file to monitor>");
            System.exit(1);
        }
        final String rrdFile = args[0];

        // Initialize Spark.
        final SparkPlatform sparkPlatform = SparkPlatform.getInstance();
        final org.apache.spark.api.java.JavaSparkContext sc = sparkPlatform.getSparkContext(
                new Configuration(),
                Collections.singleton(ReflectionUtils.getDeclaringJar(Main.class))
        );

        // Prepare some test data.
        final int numInputDataQuanta = 1000000;
        final Supplier<Integer> randomIntegerSupplier = DataGenerators.createRandomIntegerSupplier(0, 10000, new Random(42));
        List<Integer> inputData = new ArrayList<>(numInputDataQuanta);
        while (inputData.size() < numInputDataQuanta) {
            inputData.add(randomIntegerSupplier.get());
        }
        final JavaRDD<Integer> inputRdd = sc.parallelize(inputData).coalesce(100, true).cache();
        inputRdd.foreach(integer -> {});
        ProfilingUtils.sleep(5000);

        // Let's measure wall-clock time for now.

        long startTime = System.currentTimeMillis();
        inputRdd.reduce(Math::max);
        long endTime = System.currentTimeMillis();

        System.out.printf("Profiling finished in %s.\n", Formats.formatDuration(endTime - startTime));

        double load = Double.NaN;
        do {
            ProfilingUtils.sleep(5000);
            try (RrdAccessor rrdAccessor = RrdAccessor.open(rrdFile)) {
                final long lastUpdateMillis = rrdAccessor.getLastUpdateMillis();
                if (lastUpdateMillis >= endTime) {
                    load = rrdAccessor.query("sum", startTime, endTime, ConsolFun.AVERAGE);
                } else {
                    System.out.printf("Last RRD file update is from %s.\n", new Date(lastUpdateMillis));
                }
            }
        } while (Double.isNaN(load));

        System.out.printf("Fetched metric value from RRD: %f.\n", load);
    }

}
