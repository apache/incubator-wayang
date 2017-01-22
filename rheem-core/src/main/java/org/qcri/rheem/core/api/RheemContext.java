package org.qcri.rheem.core.api;

import de.hpi.isg.profiledb.store.model.Experiment;
import de.hpi.isg.profiledb.store.model.Subject;
import org.qcri.rheem.core.monitor.Monitor;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator;
import org.qcri.rheem.core.plan.executionplan.ExecutionPlan;
import org.qcri.rheem.core.plan.rheemplan.RheemPlan;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.core.profiling.CardinalityRepository;
import org.qcri.rheem.core.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        this.configuration = configuration.fork(String.format("RheemContext(%s)", configuration.getName()));
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
     * @return this instance
     */
    public RheemContext withPlugin(Plugin plugin) {
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
        this.execute(jobName, null, rheemPlan, udfJars);
    }

    /**
     * Execute a plan.
     *
     * @param jobName   name of the {@link Job} or {@code null}
     * @param rheemPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, Monitor monitor, RheemPlan rheemPlan, String... udfJars) {
        this.createJob(jobName, monitor, rheemPlan, udfJars).execute();
    }

    /**
     * Execute a plan.
     *
     * @param jobName    name of the {@link Job} or {@code null}
     * @param rheemPlan  the plan to execute
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @param udfJars    JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public void execute(String jobName, RheemPlan rheemPlan, Experiment experiment, String... udfJars) {
        this.createJob(jobName, rheemPlan, experiment, udfJars).execute();
    }


    /**
     * Build an execution plan.
     *
     * @param rheemPlan the plan to translate
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public ExecutionPlan buildInitialExecutionPlan(String jobName, RheemPlan rheemPlan, String... udfJars) {
        return this.createJob(jobName, null, rheemPlan, udfJars).buildInitialExecutionPlan();
    }

    /**
     * Create a new {@link Job} that should execute the given {@link RheemPlan} eventually.
     *
     * @param experiment {@link Experiment} for that profiling entries will be created
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, RheemPlan rheemPlan, Experiment experiment, String... udfJars) {
        return new Job(this, jobName, null, rheemPlan, experiment, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link RheemPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, RheemPlan rheemPlan, String... udfJars) {
        return this.createJob(jobName, null, rheemPlan, udfJars);
    }

    /**
     * Create a new {@link Job} that should execute the given {@link RheemPlan} eventually.
     *
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    public Job createJob(String jobName, Monitor monitor, RheemPlan rheemPlan, String... udfJars) {
        return new Job(this, jobName, monitor, rheemPlan, new Experiment("unknown", new Subject("unknown", "unknown")), udfJars);
    }

    public Configuration getConfiguration() {
        return this.configuration;
    }

    public CardinalityRepository getCardinalityRepository() {
        if (this.cardinalityRepository == null) {
            this.cardinalityRepository = new CardinalityRepository(this.configuration);
        }
        return this.cardinalityRepository;
    }
}
