package org.qcri.rheem.profiler.spark;

import de.hpi.isg.profiledb.instrumentation.StopWatch;
import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import de.hpi.isg.profiledb.store.model.TimeMeasurement;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.spark.platform.SparkPlatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Starts a profiling run of Spark.
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.printf("Usage: java %s <operator to profile> [<cardinality n>[,<cardinality n>]*]+ \n", Main.class);
            System.exit(1);
        }

        String operator = args[0];
        List<List<Long>> allCardinalities = new LinkedList<>();
        for (int i = 1; i < args.length; i++) {
            List<Long> cardinalities = Arrays.stream(args[i].split(",")).map(Long::valueOf).collect(Collectors.toList());
            allCardinalities.add(cardinalities);
        }
        List<SparkOperatorProfiler.Result> results;

        switch (operator) {
            case "textsource":
                results = profile(OperatorProfilers.createSparkTextFileSourceProfiler(), allCardinalities);
                break;
            case "collectionsource":
                results = profile(OperatorProfilers.createSparkCollectionSourceProfiler(), allCardinalities);
                break;
            case "map":
                results = profile(OperatorProfilers.createSparkMapProfiler(), allCardinalities);
                break;
            case "filter":
                results = profile(OperatorProfilers.createSparkFilterProfiler(), allCardinalities);
                break;
            case "flatmap":
                results = profile(OperatorProfilers.createSparkFlatMapProfiler(), allCardinalities);
                break;
            case "reduce":
                results = profile(OperatorProfilers.createSparkReduceByProfiler(), allCardinalities);
                break;
            case "globalreduce":
                results = profile(OperatorProfilers.createSparkGlobalReduceProfiler(), allCardinalities);
                break;
            case "distinct":
            case "distinct-string":
                results = profile(OperatorProfilers.createSparkDistinctProfiler(), allCardinalities);
                break;
            case "distinct-integer":
                results = profile(OperatorProfilers.createSparkDistinctProfiler(
                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                        Integer.class,
                        new Configuration()
                ), allCardinalities);
                break;
            case "sort":
            case "sort-string":
                results = profile(OperatorProfilers.createSparkSortProfiler(), allCardinalities);
                break;
            case "sort-integer":
                results = profile(OperatorProfilers.createSparkSortProfiler(
                        DataGenerators.createReservoirBasedIntegerSupplier(new ArrayList<>(), 0.7d, new Random(42)),
                        Integer.class,
                        new Configuration()
                ), allCardinalities);
                break;
            case "count":
                results = profile(OperatorProfilers.createSparkCountProfiler(), allCardinalities);
                break;
            case "groupby":
                results = profile(OperatorProfilers.createSparkMaterializedGroupByProfiler(), allCardinalities);
                break;
            case "join":
                results = profile(OperatorProfilers.createSparkJoinProfiler(), allCardinalities);
                break;
            case "union":
                results = profile(OperatorProfilers.createSparkUnionProfiler(), allCardinalities);
                break;
            case "cartesian":
                results = profile(OperatorProfilers.createSparkCartesianProfiler(), allCardinalities);
                break;
            case "callbacksink":
                results = profile(OperatorProfilers.createSparkLocalCallbackSinkProfiler(), allCardinalities);
                break;
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

    private static StopWatch createStopWatch() {
        Experiment experiment = new Experiment("rheem-profiler", new Subject("Rheem", "0.1"));
        return new StopWatch(experiment);
    }

    /**
     * Run the {@code opProfiler} with all combinations that can be derived from {@code allCardinalities}.
     */
    private static List<SparkOperatorProfiler.Result> profile(SparkOperatorProfiler opProfiler,
                                                              List<List<Long>> allCardinalities) {

        return StreamSupport.stream(RheemCollections.streamedCrossProduct(allCardinalities).spliterator(), false)
                .map(cardinalities -> profile(opProfiler, RheemArrays.toArray(cardinalities)))
                .collect(Collectors.toList());


    }

    /**
     * Run the {@code opProfiler} with the given {@code cardinalities}.
     */
    private static SparkOperatorProfiler.Result profile(SparkOperatorProfiler opProfiler, long... cardinalities) {
        System.out.printf("Profiling %s with %s data quanta.\n", opProfiler, RheemArrays.asList(cardinalities));
        final StopWatch stopWatch = createStopWatch();
        SparkOperatorProfiler.Result result = null;

        try {
            System.out.println("Prepare...");
            final TimeMeasurement preparation = stopWatch.start("Preparation");
            SparkPlatform.getInstance().warmUp(new Configuration());
            opProfiler.prepare(cardinalities);
            preparation.stop();

            System.out.println("Execute...");
            final TimeMeasurement execution = stopWatch.start("Execution");
            result = opProfiler.run();
            execution.stop();
        } finally {
            System.out.println("Clean up...");
            final TimeMeasurement cleanUp = stopWatch.start("Clean up");
            opProfiler.cleanUp();
            cleanUp.stop();

            System.out.println("Measurement:");
            if (result != null) System.out.println(result);
            System.out.println(stopWatch.toPrettyString());
            System.out.println();
        }


        return result;
    }

}
