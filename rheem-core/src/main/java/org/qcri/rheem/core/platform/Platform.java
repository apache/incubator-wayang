package org.qcri.rheem.core.platform;

import org.json.JSONObject;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.optimizer.costs.TimeToCostConverter;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.util.JsonSerializables;
import org.qcri.rheem.core.util.JsonSerializer;
import org.qcri.rheem.core.util.ReflectionUtils;

/**
 * A platform describes an execution engine that executes {@link ExecutionOperator}s.
 */
public abstract class Platform {

    private final String name, configName;

    /**
     * Loads a specific {@link Platform} implementation. For platforms to interoperate with this method, they must
     * provide a {@code static}, parameterless method {@code getInstance()} that returns their singleton instance.
     *
     * @param platformClassName the class name of the {@link Platform}
     * @return the {@link Platform} instance
     */
    public static Platform load(String platformClassName) {
        try {
            return ReflectionUtils.executeStaticArglessMethod(platformClassName, "getInstance");
        } catch (Exception e) {
            throw new RheemException("Could not load platform: " + platformClassName, e);
        }
    }

    protected Platform(String name, String configName) {
        this.name = name;
        this.configName = configName;
        this.configureDefaults(Configuration.getDefaultConfiguration());
    }

    /**
     * Configure default settings for this instance, e.g., to be able to create {@link LoadProfileToTimeConverter}s.
     *
     * @param configuration that should be configured
     */
    protected abstract void configureDefaults(Configuration configuration);

    /**
     * <i>Shortcut.</i> Creates an {@link Executor} using the {@link #getExecutorFactory()}.
     *
     * @return the {@link Executor}
     */
    public Executor createExecutor(Job job) {
        return this.getExecutorFactory().create(job);
    }

    public abstract Executor.Factory getExecutorFactory();

    public String getName() {
        return this.name;
    }

    /**
     * Retrieve the name of this instance as it is used in {@link Configuration} keys.
     *
     * @return the configuration name of this instance
     */
    public String getConfigurationName() {
        return this.configName;
    }

    // TODO: Return some more descriptors about the state of the platform (e.g., available machines, RAM, ...)?

    @Override
    public String toString() {
        return String.format("Platform[%s]", this.getName());
    }

    /**
     * Tells whether the given constellation of producing and consuming {@link ExecutionTask}, linked by the
     * {@link Channel} can be handled within a single {@link PlatformExecution} of this {@link Platform}
     *
     * @param producerTask an {@link ExecutionTask} running on this {@link Platform}
     * @param channel      links the {@code producerTask} and {@code consumerTask}
     * @param consumerTask an  {@link ExecutionTask} running on this {@link Platform}
     * @return whether the {@link ExecutionTask}s can be executed in a single {@link PlatformExecution}
     */
    public boolean isSinglePlatformExecutionPossible(ExecutionTask producerTask, Channel channel, ExecutionTask consumerTask) {
        assert producerTask.getOperator().getPlatform() == this;
        assert consumerTask.getOperator().getPlatform() == this;
        assert channel.getProducer() == producerTask;
        assert channel.getConsumers().contains(consumerTask);

        // Overwrite as necessary.
        return true;
    }

    /**
     * @return a default {@link LoadProfileToTimeConverter}
     */
    public abstract LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration);

    /**
     * Creates a {@link TimeToCostConverter} for this instance.
     *
     * @param configuration configures the {@link TimeToCostConverter}
     * @return the {@link TimeToCostConverter}
     */
    public abstract TimeToCostConverter createTimeToCostConverter(Configuration configuration);

    /**
     * Warm up this instance.
     *
     * @param configuration used to warm up
     */
    public void warmUp(Configuration configuration) {
        // Do nothing by default.
    }

    /**
     * Get the time necessary to initialize this instance and use it for execution.
     *
     * @return the milliseconds required to initialize this instance
     */
    public long getInitializeMillis(Configuration configuration) {
        return 0L;
    }

    /**
     * Default {@link JsonSerializer} implementation that stores the {@link Class} of the instance and then
     * tries to deserialize by invoking the static {@code getInstance()} method.
     */
    public static final JsonSerializer<Platform> jsonSerializer = new JsonSerializer<Platform>() {

        @Override
        public JSONObject serialize(Platform platform) {
            // Enforce polymorph serialization.
            return JsonSerializables.addClassTag(platform, new JSONObject());
        }

        @Override
        public Platform deserialize(JSONObject json, Class<? extends Platform> cls) {
            return ReflectionUtils.evaluate(cls.getCanonicalName() + ".getInstance()");
        }
    };
}
