/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.platform;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;

import java.lang.reflect.Constructor;
import java.util.Objects;

/**
 * Describes a certain {@link Channel} type including further parameters.
 */
public class ChannelDescriptor {

    private final Class<? extends Channel>
            channelClass;

    private final boolean isReusable;

    /**
     * Tells whether corresponding {@link Channel}s are suited to be between {@link ExecutionStage}s and fit with
     * {@link Breakpoint}s.
     *
     * @see Channel#isSuitableForBreakpoint()
     */
    private final boolean isSuitableForBreakpoint;

    public ChannelDescriptor(Class<? extends Channel> channelClass,
                             boolean isReusable,
                             boolean isSuitableForBreakpoint) {
        this.channelClass = channelClass;
        this.isReusable = isReusable;
        this.isSuitableForBreakpoint = isSuitableForBreakpoint;
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
                this.isSuitableForBreakpoint == that.isSuitableForBreakpoint &&
                Objects.equals(this.channelClass, that.channelClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.channelClass, this.isReusable, this.isSuitableForBreakpoint);
    }

    @Override
    public String toString() {
        return String.format("%s[%s,%s%s]",
                this.getClass().getSimpleName(),
                this.getChannelClass().getSimpleName(),
                this.isReusable() ? "r" : "-",
                this.isSuitableForBreakpoint() ? "b" : "-"
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
     * Tells whether corresponding {@link Channel}s are suited to be between {@link ExecutionStage}s and fit with
     * {@link Breakpoint}s.
     *
     * @return whether corresponding {@link Channel}s are suited to be between {@link ExecutionStage}s
     */
    public boolean isSuitableForBreakpoint() {
        return this.isSuitableForBreakpoint;
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
