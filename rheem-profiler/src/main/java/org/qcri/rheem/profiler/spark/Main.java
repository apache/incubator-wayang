package org.qcri.rheem.profiler.spark;

import org.apache.spark.api.java.JavaRDD;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.profiler.data.DataGenerators;
import org.qcri.rheem.profiler.java.OperatorProfiler;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.rrd4j.ConsolFun;

import java.util.*;
import java.util.function.Supplier;

/**
 * Starts a profiling run of Spark.
 */
public class Main {

    public static void main(String[] args) {
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
        OperatorProfiler.sleep(5000);

        // Let's measure wall-clock time for now.

        long startTime = System.currentTimeMillis();
        inputRdd.reduce(Math::max);
        long endTime = System.currentTimeMillis();

        System.out.printf("Profiling finished in %s.\n", Formats.formatDuration(endTime - startTime));

        double load = Double.NaN;
        do {
            OperatorProfiler.sleep(5000);
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
