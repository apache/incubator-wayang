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

package org.apache.wayang.core.optimizer.channels;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.platform.ChannelDescriptor;

import java.util.Collection;
import java.util.Collections;

/**
 * Can convert a given {@link Channel} to another {@link Channel}.
 */
public abstract class ChannelConversion {

    private final ChannelDescriptor sourceChannelDescriptor;

    private final ChannelDescriptor targetChannelDescriptor;

    public ChannelConversion(ChannelDescriptor sourceChannelDescriptor, ChannelDescriptor targetChannelDescriptor) {
        this.sourceChannelDescriptor = sourceChannelDescriptor;
        this.targetChannelDescriptor = targetChannelDescriptor;
    }

    /**
     * Converts the given {@code sourceChannel} into a new {@link Channel} according to {@link #targetChannelDescriptor}.
     *
     * @param sourceChannel the {@link Channel} to be converted
     * @param configuration can provide additional information for setting up {@link Channel}s etc.
     * @return the newly created {@link Channel}
     */
    public Channel convert(final Channel sourceChannel, Configuration configuration) {
        return this.convert(sourceChannel, configuration, Collections.emptyList(), null);
    }

    /**
     * Converts the given {@code sourceChannel} into a new {@link Channel} according to {@link #targetChannelDescriptor}.
     *
     * @param sourceChannel        the {@link Channel} to be converted
     * @param configuration        can provide additional information for setting up {@link Channel}s etc.
     * @param optimizationContexts to which estimates of the newly added {@link Operator} should be added
     * @param cardinality          optional {@link CardinalityEstimate} of the {@link Channel}
     * @return the newly created {@link Channel}
     */
    public abstract Channel convert(final Channel sourceChannel,
                                    final Configuration configuration,
                                    final Collection<OptimizationContext> optimizationContexts,
                                    final CardinalityEstimate cardinality);

    /**
     * Update an already existing {@link ChannelConversion}.
     *
     * @param sourceChannel        the {@link Channel} to be converted
     * @param targetChannel        the converted {@link Channel}
     * @param optimizationContexts to which estimates of the newly added {@link Operator} should be updated
     * @param cardinality          optional {@link CardinalityEstimate} of the {@link Channel}s
     */
    public abstract void update(final Channel sourceChannel,
                                final Channel targetChannel,
                                final Collection<OptimizationContext> optimizationContexts,
                                final CardinalityEstimate cardinality);

    public ChannelDescriptor getSourceChannelDescriptor() {
        return this.sourceChannelDescriptor;
    }

    public ChannelDescriptor getTargetChannelDescriptor() {
        return this.targetChannelDescriptor;
    }


    /**
     * Estimate the required cost to carry out the conversion for a given {@code cardinality}.
     *
     * @param cardinality         the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions       expected number of executions of this instance
     * @param optimizationContext provides a {@link Configuration} and keeps around generated optimization information
     * @return the cost estimate
     */
    public abstract ProbabilisticDoubleInterval estimateConversionCost(CardinalityEstimate cardinality,
                                                                       int numExecutions,
                                                                       OptimizationContext optimizationContext);

    /**
     * Determine whether this instance is suitable to handle the given conversion.
     *
     * @param cardinality         the {@link CardinalityEstimate} of data to be converted
     * @param numExecutions       expected number of executions of this instance
     * @param optimizationContext provides a {@link Configuration} and keeps around generated optimization information
     * @return whether this instance should be filtered
     */
    public abstract boolean isFiltered(CardinalityEstimate cardinality,
                                       int numExecutions,
                                       OptimizationContext optimizationContext);

    @Override
    public String toString() {
        return String.format("%s[%s->%s]",
                this.getClass().getSimpleName(),
                this.getSourceChannelDescriptor().getChannelClass().getSimpleName(),
                this.getTargetChannelDescriptor().getChannelClass().getSimpleName()
        );
    }
}
