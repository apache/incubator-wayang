package org.qcri.rheem.java.profiler;

import org.qcri.rheem.core.function.TransformationDescriptor;
import org.qcri.rheem.core.optimizer.costs.LoadProfileEstimator;
import org.qcri.rheem.core.types.DataSetType;
import org.qcri.rheem.core.util.StopWatch;
import org.qcri.rheem.java.operators.JavaExecutionOperator;
import org.qcri.rheem.java.operators.JavaMapOperator;

import java.util.Random;

/**
 * Utility to support finding reasonable {@link LoadProfileEstimator}s for {@link JavaExecutionOperator}s.
 */
public class Profiler {

    public static void main(String[] args) {


        // Profile the JavaMapOperator operator.
        final Random random = new Random(42);
        final JavaMapProfiler<Integer, Integer> javaMapProfiler = new JavaMapProfiler<>(
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

        for (int cardinality : new int[] { 10, 100, 1000, 10000, 100000, 1000000, 10000000 }) {
            System.out.printf("Profiling %s with %d Integers.\n",
                    JavaMapOperator.class, cardinality);
            sleep(1000);
            final StopWatch stopWatch = new StopWatch();

            System.out.println("Prepare...");
            final StopWatch.Round preparation = stopWatch.start("Preparation");
            javaMapProfiler.prepare(cardinality);
            preparation.stop();

            System.out.println("Execute...");
            final StopWatch.Round execution = stopWatch.start("Execution");
            javaMapProfiler.run();
            execution.stop();

            System.out.println(stopWatch.toPrettyString());
            System.out.println();
        }



    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
