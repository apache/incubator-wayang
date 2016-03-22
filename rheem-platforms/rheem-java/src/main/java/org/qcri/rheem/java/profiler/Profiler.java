package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.function.FlatMapDescriptor;
import org.qcri.rheem.core.function.PredicateDescriptor;
import org.qcri.rheem.core.function.ReduceDescriptor;
import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.RheemArrays;
import org.qcri.rheem.core.util.RheemCollections;
import org.qcri.rheem.core.util.StopWatch;
import org.qcri.rheem.java.operators.*;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Utility to support finding reasonable {@link LoadProfileEstimator}s for {@link JavaExecutionOperator}s.
 */
public class Profiler {

    private static final String[] CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789".split("");

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.printf("Usage: java %s <operator to profile> <cardinality>[,<cardinality>]\n", Profiler.class);
            System.exit(1);
        }

        String operator = args[0];
        List<Integer> cardinalities = Arrays.stream(args[1].split(",")).map(Integer::valueOf).collect(Collectors.toList());
        List<OperatorProfiler.Result> results;

        switch (operator) {
            case "map":
                results = profile(createJavaMapProfiler(), cardinalities);
                break;
            case "filter":
                results = profile(createJavaFilterProfiler(), cardinalities);
                break;
            case "flatmap":
                results = profile(createJavaFlatMapProfiler(), cardinalities);
                break;
            case "reduce":
                results = profile(createJavaReduceProfiler(), cardinalities);
                break;
            case "join":
                results = profile(createJavaJoinProfiler(), cardinalities, cardinalities);
                break;
            case "union":
                results = profile(createJavaUnionProfiler(), cardinalities, cardinalities);
                break;
            default:
                System.out.println("Unknown operator: " + operator);
                return;
        }

        System.out.println();
        System.out.println(RheemCollections.getAny(results).getCsvHeader());
        results.forEach(result -> System.out.println(result.toCsvString()));
    }

    private static UnaryOperatorProfiler<Integer> createJavaMapProfiler() {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler<>(
                random::nextInt,
                () -> new JavaMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new TransformationDescriptor<>(
                                integer -> integer,
                                Integer.class,
                                Integer.class
                        )
                )
        );
    }

    private static UnaryOperatorProfiler<Integer> createJavaFlatMapProfiler() {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler<>(
                random::nextInt,
                () -> new JavaFlatMapOperator<>(
                        DataSetType.createDefault(Integer.class),
                        DataSetType.createDefault(Integer.class),
                        new FlatMapDescriptor<>(
                                RheemArrays::asList,
                                Integer.class,
                                Integer.class
                        )
                )
        );
    }

    private static UnaryOperatorProfiler<Integer> createJavaFilterProfiler() {
        final Random random = new Random(42);
        return new UnaryOperatorProfiler<>(
                random::nextInt,
                () -> new JavaFilterOperator<>(
                        DataSetType.createDefault(Integer.class),
                        new PredicateDescriptor<>(i -> (i & 1) == 0, Integer.class)
                )
        );
    }


    private static UnaryOperatorProfiler<String> createJavaReduceProfiler() {
        final Random random = new Random(42);
        final Supplier<String> stringSupplier = createReservoirBasedStringSupplier(new ArrayList<>(), 0.7, random, 4, 20);
        return new UnaryOperatorProfiler<>(
                stringSupplier,
                () -> new JavaReduceByOperator<>(
                        DataSetType.createDefault(String.class),
                        new TransformationDescriptor<>(String::new, String.class, String.class),
                        new ReduceDescriptor<>((s1, s2) -> s1, String.class)
                )
        );
    }

    private static BinaryOperatorProfiler<String, String> createJavaJoinProfiler() {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random(42);
        final int minLen = 4, maxLen = 6;
        Supplier<String> reservoirStringSupplier = createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, minLen, maxLen);
        return new BinaryOperatorProfiler<>(
                reservoirStringSupplier,
                reservoirStringSupplier,
                () -> new JavaJoinOperator<>(
                        DataSetType.createDefault(String.class),
                        DataSetType.createDefault(String.class),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        ),
                        new TransformationDescriptor<>(
                                String::new,
                                String.class,
                                String.class
                        )
                )
        );
    }

    private static BinaryOperatorProfiler<String, String> createJavaUnionProfiler() {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random(42);
        Supplier<String> reservoirStringSupplier = createReservoirBasedStringSupplier(stringReservoir, reuseProbability, random, 4, 6);
        return new BinaryOperatorProfiler<>(
                reservoirStringSupplier,
                reservoirStringSupplier,
                () -> new JavaUnionAllOperator<>(DataSetType.createDefault(String.class))
        );
    }

    private static Supplier<String> createReservoirBasedStringSupplier(List<String> stringReservoir,
                                                                       double reuseProbability,
                                                                       Random random,
                                                                       int minLen,
                                                                       int maxLen) {
        return () -> {
            if (random.nextDouble() > reuseProbability || stringReservoir.isEmpty()) {
                final String randomString = createRandomString(minLen, maxLen, random);
                stringReservoir.add(randomString);
                return randomString;
            } else {
                return stringReservoir.get(random.nextInt(stringReservoir.size()));
            }
        };
    }

    private static String createRandomString(int minLen, int maxLen, Random random) {
        int len = (minLen == maxLen) ? minLen : (random.nextInt(maxLen - minLen) + minLen);
        StringBuilder sb = new StringBuilder(len);
        while (sb.length() < len) {
            sb.append(CHARACTERS[random.nextInt(CHARACTERS.length)]);
        }
        return sb.toString();
    }

    private static <T> List<OperatorProfiler.Result> profile(UnaryOperatorProfiler<T> unaryOperatorProfiler, Collection<Integer> cardinalities) {
        return cardinalities.stream()
                .map(cardinality -> profile(unaryOperatorProfiler, cardinality))
                .collect(Collectors.toList());
    }

    private static <T> OperatorProfiler.Result profile(UnaryOperatorProfiler<T> unaryOperatorProfiler, int cardinality) {
        System.out.printf("Profiling %s with %d data quanta.\n", unaryOperatorProfiler, cardinality);
        final StopWatch stopWatch = new StopWatch();

        System.out.println("Prepare...");
        final StopWatch.Round preparation = stopWatch.start("Preparation");
        unaryOperatorProfiler.prepare(cardinality);
        preparation.stop();

        System.out.println("Execute...");
        final StopWatch.Round execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = unaryOperatorProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }

    private static <T, U> List<OperatorProfiler.Result> profile(BinaryOperatorProfiler<T, U> binaryOperatorProfiler,
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

    private static <T, U> OperatorProfiler.Result profile(BinaryOperatorProfiler<T, U> binaryOperatorProfiler,
                                                          int cardinality0,
                                                          int cardinality1) {
        System.out.printf("Profiling %s with %dx%d data quanta.\n", binaryOperatorProfiler.getOperator(), cardinality0, cardinality1);
        final StopWatch stopWatch = new StopWatch();

        System.out.println("Prepare...");
        final StopWatch.Round preparation = stopWatch.start("Preparation");
        binaryOperatorProfiler.prepare(cardinality0, cardinality1);
        preparation.stop();

        System.out.println("Execute...");
        final StopWatch.Round execution = stopWatch.start("Execution");
        final OperatorProfiler.Result result = binaryOperatorProfiler.run();
        execution.stop();

        System.out.println("Measurement:");
        System.out.println(result);
        System.out.println(stopWatch.toPrettyString());
        System.out.println();

        return result;
    }


}
