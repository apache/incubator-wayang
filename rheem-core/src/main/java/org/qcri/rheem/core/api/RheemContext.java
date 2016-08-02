package org.qcri.rheem.core.api;

import org.apache.commons.lang3.StringUtils;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.profiling.CardinalityRepository;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

/**
 * This is the entry point for users to work with Rheem.
 */
public class RheemContext {

    @SuppressWarnings("unused")
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Stores input/output cardinalities to provide better {@link CardinalityEstimator}s over time.
     */
    private CardinalityRepository cardinalityRepository;

    private final Configuration configuration;

    public RheemContext() {
        this(new Configuration());
    }

    public RheemContext(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @return this instance
     */
    public RheemContext with(Plugin plugin) {
        this.register(plugin);
        return this;
    }

    /**
     * Registers the given {@link Plugin} with this instance.
     *
     * @param plugin the {@link Plugin} to register
     * @see #with(Plugin)
     */
    public void register(Plugin plugin) {
        plugin.configure(this.getConfiguration());
    }

    /**
     * Execute a plan.
     *
     * @param rheemPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(RheemPlan rheemPlan, String... udfJars) {
        this.execute(null, rheemPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param rheemPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, RheemPlan rheemPlan, String... udfJars) {
        this.createJob(jobName, rheemPlan, udfJars).execute();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link RheemPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, RheemPlan rheemPlan, String... udfJars) {
        return new Job(this, jobName, rheemPlan, udfJars);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public CardinalityRepository getCardinalityRepository() {
        if (this.cardinalityRepository == null) {
            final File repoFile = new File(StringUtils.join(
                    Arrays.asList(System.getProperty("user.home"), ".rheem", "cardinality-repository.json"),
                    File.separator
            ));
            this.cardinalityRepository = new CardinalityRepository(repoFile.getPath());
        }
        return this.cardinalityRepository;
    }
}
