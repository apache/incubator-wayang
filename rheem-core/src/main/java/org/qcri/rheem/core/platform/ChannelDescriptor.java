package org.qcri.rheem.core.platform;

import org.qcri.rheem.core.plan.executionplan.Channel;

import java.util.Objects;

/**
 * Describes a certain {@link Channel} type including further parameters.
 */
public class ChannelDescriptor {

    private final Class<? extends Channel> channelClass;

    public ChannelDescriptor(Class<? extends Channel> channelClass) {
        this.channelClass = channelClass;
    }

    public Class<? extends Channel> getChannelClass() {
        return this.channelClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;
        ChannelDescriptor that = (ChannelDescriptor) o;
        return Objects.equals(this.channelClass, that.channelClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.channelClass);
    }

    @Override
    public String toString() {
        return String.format("%s[%s]", this.getClass().getSimpleName(), this.getChannelClass().getSimpleName());
    }

}
