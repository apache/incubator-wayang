package org.qcri.rheem.profiler.util;

import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.util.Formats;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.execution.SparkExecutor;
import org.qcri.rheem.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

/**
 * Utilities to fake Rheem internals etc..
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
        return new RheemContext().createJob("Fake job", new RheemPlan(), udfJars);
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
