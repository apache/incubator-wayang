package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.plan.executionplan.Channel;
import org.qcri.rheem.core.plan.executionplan.ExecutionStage;
import org.qcri.rheem.core.plan.executionplan.PlatformExecution;
import org.qcri.rheem.core.plan.rheemplan.OutputSlot;

import java.lang.reflect.Constructor;
import java.util.Objects;

/**
 * Describes a certain {@link Channel} type including further parameters.
 */
public class ChannelDescriptor {

    private final Class<? extends Channel>
            channelClass;

    private final boolean isReusable;

    private final boolean isInterStageCable;

    private final boolean isInterPlatformCapable;

    public ChannelDescriptor(Class<? extends Channel> channelClass,
                             boolean isReusable,
                             boolean isInterStageCable,
                             boolean isInterPlatformCapable) {
        this.channelClass = channelClass;
        this.isReusable = isReusable;
        this.isInterStageCable = isInterStageCable;
        this.isInterPlatformCapable = isInterPlatformCapable;
    }

    public Class<? extends Channel> getChannelClass() {
        return this.channelClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        ChannelDescriptor that = (ChannelDescriptor) o;
        return this.isReusable == that.isReusable &&
                this.isInterStageCable == that.isInterStageCable &&
                this.isInterPlatformCapable == that.isInterPlatformCapable &&
                Objects.equals(this.channelClass, that.channelClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.channelClass, this.isReusable, this.isInterStageCable, this.isInterPlatformCapable);
    }

    @Override
    public String toString() {
        return String.format("%s[%s,%s%s%s]",
                this.getClass().getSimpleName(),
                this.getChannelClass().getSimpleName(),
                this.isReusable() ? "r" : "-",
                this.isInterStageCapable() ? "s" : "-",
                this.isInterPlatformCapable() ? "p" : "-"
        );
    }


    /**
     * Declares whether this is not a read-once instance.
     *
     * @return whether this instance can have multiple consumers
     */
    public boolean isReusable() {
        return this.isReusable;
    }

    /**
     * Declares whether this instance can be shared among two different {@link ExecutionStage}s (of the same
     * {@link PlatformExecution}, though).
     */
    public boolean isInterStageCapable() {
        return this.isInterStageCable;
    }

    /**
     * Declares whether this instance can be shared among two different {@link PlatformExecution}s.
     */
    public boolean isInterPlatformCapable() {
        return this.isInterPlatformCapable;
    }

    /**
     * Declares whether the {@link Channel} is a {@link Platform}-internal.
     */
    public boolean isInternal() {
        return !this.isInterPlatformCapable();
    }

    /**
     * Creates a new {@link Channel} as described by this instance.
     *
     * @param configuration can provide additional information required for the creation
     * @return the {@link Channel}
     */
    public Channel createChannel(OutputSlot<?> output, Configuration configuration) {
        Class<? extends Channel> channelClass = this.getChannelClass();
        try {
            final Constructor<? extends Channel> constructor = channelClass.getConstructor(ChannelDescriptor.class, OutputSlot.class);
            return constructor.newInstance(this, output);
        } catch (Exception e) {
            throw new UnsupportedOperationException(
                    String.format("Default channel creation is not working for %s. Please override.", this.getChannelClass().getSimpleName()),
                    e
            );
        }
    }
}
