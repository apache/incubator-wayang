package org.qcri.rheem.spark.plugin;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.spark.platform.SparkExecutor;

/**
 * Activator for the Spark platform binding for Rheem.
 */
public class Activator {

    public static final Platform PLATFORM = new Platform("spark", SparkExecutor.FACTORY);

    // TODO set spark config properly.
    public static final SparkConf conf = new SparkConf().setAppName("rheem").setMaster("local");
    public static final JavaSparkContext sc = new JavaSparkContext(conf);

    public static void activate(RheemContext rheemContext) {
    }

}
