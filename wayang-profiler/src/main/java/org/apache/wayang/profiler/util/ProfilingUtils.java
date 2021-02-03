package org.apache.wayang.profiler.util;

import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.Formats;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.execution.JavaExecutor;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.execution.SparkExecutor;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

/**
 * Utilities to fake Wayang internals etc..
 */
public class ProfilingUtils {

    private ProfilingUtils() {
    }

    /**
     * Provides a {@link Job}.
     *
     * @param udfJars paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    public static Job fakeJob(String... udfJars) {
        return new WayangContext().createJob("Fake job", new WayangPlan(), udfJars);
    }

    /**
     * Provides a {@link SparkExecutor}.
     *
     * @param udfJars paths to JAR files needed to run the UDFs (see {@link ReflectionUtils#getDeclaringJar(Class)})
     */
    public static SparkExecutor fakeSparkExecutor(String... udfJars) {
        return (SparkExecutor) SparkPlatform.getInstance().createExecutor(fakeJob(udfJars));
    }

    /**
     * Provides a {@link JavaExecutor}.
     */
    public static JavaExecutor fakeJavaExecutor() {
        return (JavaExecutor) JavaPlatform.getInstance().createExecutor(fakeJob());
    }

    /**
     * Puts the current {@link Thread} to sleep for a given number of milliseconds. Notifies the user via the
     * {@link System#out}.
     */
    public static void sleep(long millis) {
        try {
            System.out.printf("Sleeping for %s.\n", Formats.formatDuration(millis));
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            LoggerFactory.getLogger(ProfilingUtils.class).error("Sleep interrupted.", e);
        }
    }
}
