package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.StopWatch;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.operators.JavaJoinOperator;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
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

        switch (operator) {
            case "map":
                profile(createJavaMapProfiler(), cardinalities);
                break;
            case "join":
                profile(createJavaJoinProfiler(), cardinalities, cardinalities);
                break;
        }
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

    private static BinaryOperatorProfiler<String, String> createJavaJoinProfiler() {
        final List<String> stringReservoir = new ArrayList<>();
        final double reuseProbability = 0.3;
        final Random random = new Random(42);
        Supplier<String> reservoirStringSupplier =
                () -> {
                    if (random.nextDouble() > reuseProbability || stringReservoir.isEmpty()) {
                        final String randomString = createRandomString(4, 6, random);
                        stringReservoir.add(randomString);
                        return randomString;
                    } else {
                        return stringReservoir.get(random.nextInt(stringReservoir.size()));
                    }
                };
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

    private static String createRandomString(int minLen, int maxLen, Random random) {
        int len = (minLen == maxLen) ? minLen : (random.nextInt(maxLen - minLen) + minLen);
        StringBuilder sb = new StringBuilder(len);
        while (sb.length() < len) {
            sb.append(CHARACTERS[random.nextInt(CHARACTERS.length)]);
        }
        return sb.toString();
    }

    private static <T> void profile(UnaryOperatorProfiler<T> unaryOperatorProfiler, Collection<Integer> cardinalities) {
        cardinalities.forEach(cardinality -> profile(unaryOperatorProfiler, cardinality));
    }

    private static <T> void profile(UnaryOperatorProfiler<T> unaryOperatorProfiler, int cardinality) {
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
    }

    private static <T, U> void profile(BinaryOperatorProfiler<T, U> binaryOperatorProfiler,
                                       Collection<Integer> cardinalities0,
                                       Collection<Integer> cardinalities1) {
        cardinalities0.forEach(cardinality0 ->
                cardinalities1.forEach(
                        cardinality1 -> profile(binaryOperatorProfiler, cardinality0, cardinality1)
                )
        );
    }

    private static <T, U> void profile(BinaryOperatorProfiler<T, U> binaryOperatorProfiler,
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
    }



}
