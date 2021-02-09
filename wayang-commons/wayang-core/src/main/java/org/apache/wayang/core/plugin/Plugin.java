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

package org.apache.wayang.core.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;

import java.util.Collection;
import java.util.Collections;

/**
 * A plugin contributes the following components to a {@link WayangContext}:
 * <ul>
 * <li>{@link Mapping}s,</li>
 * <li>{@link ChannelConversion}s, and</li>
 * <li>{@link Configuration} properties.</li>
 * </ul>
 * In turn, it may require several {@link Platform}s for its operation.
 */
public interface Plugin {

    /**
     * Configures the {@link Configuration}, thereby registering the components of this instance.
     *
     * @param configuration that should be configured
     */
    default void configure(Configuration configuration) {
        configuration.getPlatformProvider().addAllToWhitelist(this.getRequiredPlatforms());
        configuration.getPlatformProvider().addAllToBlacklist(this.getExcludedRequiredPlatforms());
        configuration.getMappingProvider().addAllToWhitelist(this.getMappings());
        configuration.getMappingProvider().addAllToBlacklist(this.getExcludedMappings());
        configuration.getChannelConversionProvider().addAllToWhitelist(this.getChannelConversions());
        configuration.getChannelConversionProvider().addAllToBlacklist(this.getExcludedChannelConversions());
        this.setProperties(configuration);
    }

    /**
     * Provides the {@link Platform}s required by this instance.
     *
     * @return the {@link Platform}s
     */
    Collection<Platform> getRequiredPlatforms();

    /**
     * Provides the required {@link Platform}s excluded by this instance.
     *
     * @return the {@link Platform}s
     */
    default Collection<Platform> getExcludedRequiredPlatforms() {
        return Collections.emptyList();
    }

    /**
     * Provides the {@link Mapping}s shipped with this instance.
     *
     * @return the {@link Mapping}s
     */
    Collection<Mapping> getMappings();

    /**
     * Provides the {@link Mapping}s excluded by this instance.
     *
     * @return the {@link Mapping}s
     */
    default Collection<Mapping> getExcludedMappings() {
        return Collections.emptyList();
    }

    /**
     * Provides the {@link ChannelConversion}s shipped with this instance.
     *
     * @return the {@link ChannelConversion}s
     */
    Collection<ChannelConversion> getChannelConversions();

    /**
     * Provides the {@link ChannelConversion}s excluded by this instance.
     *
     * @return the {@link ChannelConversion}s
     */
    default Collection<ChannelConversion> getExcludedChannelConversions() {
        return Collections.emptyList();
    }

    /**
     * Provides relevant {@link Configuration} properties.
     *
     * @param configuration accepts the properties
     */
    void setProperties(Configuration configuration);

}
