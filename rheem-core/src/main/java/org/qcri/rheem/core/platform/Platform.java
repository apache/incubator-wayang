package org.qcri.rheem.core.platform;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.Job;
import org.qcri.rheem.core.api.exception.RheemException;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversionGraph;
import org.qcri.rheem.core.optimizer.costs.LoadProfileToTimeConverter;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionTask;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

/**
 * A platform describes an execution engine that executes {@link ExecutionOperator}s.
 */
public abstract class Platform {

    private final String name;

    /**
     * Loads a specific {@link Platform} implementation. For platforms to interoperate with this method, they must
     * provide a {@code static}, parameterless method {@code getInstance()} that returns their singleton instance.
     *
     * @param platformClassName the class name of the {@link Platform}
     * @return the {@link Platform} instance
     */
    public static Platform load(String platformClassName) {
        final Class<?> platformClass;
        try {
            platformClass = Class.forName(platformClassName);
            final Method getInstanceMethod = platformClass.getMethod("getInstance");
            return (Platform) getInstanceMethod.invoke(null);
        } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RheemException("Could not load platform: " + platformClassName, e);
        }
    }

    protected Platform(String name) {
        this.name = name;
    }

    /**
     * Registers supported todo...
     */
    public abstract void addChannelConversionsTo(ChannelConversionGraph channelConversionGraph);

    /**
     * <i>Shortcut.</i> Creates an {@link Executor} using the {@link #getExecutorFactory()}.
     *
     * @return the {@link Executor}
     */
    public Executor createExecutor(Job job) {
        Validate.isTrue(this.isExecutable());
        return this.getExecutorFactory().create(job);
    }

    public abstract Executor.Factory getExecutorFactory();

    public abstract Collection<Mapping> getMappings();

    public String getName() {
        return this.name;
    }

    public abstract boolean isExecutable();

    // TODO: Return some more descriptors about the state of the platform (e.g., available machines, RAM, ...)?

    @Override
    public String toString() {
        return String.format("Platform[%s]", this.getName());
    }

    public boolean isSinglePlatformExecutionPossible(ExecutionTask producerTask, Channel channel, ExecutionTask consumerTask) {
        assert producerTask.getOperator().getPlatform() == this;
        assert consumerTask.getOperator().getPlatform() == this;

        // Overwrite as necessary.
        return true;
    }

    /**
     * @return a default {@link LoadProfileToTimeConverter}
     */
    public abstract LoadProfileToTimeConverter createLoadProfileToTimeConverter(Configuration configuration);

}
