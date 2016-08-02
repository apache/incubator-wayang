package org.qcri.rheem.core.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.platform.Platform;

import java.util.Collection;

/**
 * A plugin contributes the following components to a {@link RheemContext}:
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
        configuration.getMappingProvider().addAllToWhitelist(this.getMappings());
        configuration.getChannelConversionProvider().addAllToWhitelist(this.getChannelConversions());
        this.setProperties(configuration);
    }

    /**
     * Provides the {@link Platform}s required by this instance.
     *
     * @return the {@link Platform}s
     */
    Collection<Platform> getRequiredPlatforms();

    /**
     * Provides the {@link Mapping}s required by this instance.
     *
     * @return the {@link Mapping}s
     */
    Collection<Mapping> getMappings();

    /**
     * Provides the {@link ChannelConversion}s required by this instance.
     *
     * @return the {@link ChannelConversion}s
     */
    Collection<ChannelConversion> getChannelConversions();

    /**
     * Provides relevant {@link Configuration} properties.
     *
     * @param configuration accepts the properties
     */
    void setProperties(Configuration configuration);

}
