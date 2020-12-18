package io.rheem.rheem.spark.plugin;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.plugin.Plugin;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.spark.mapping.Mappings;
import io.rheem.rheem.spark.platform.SparkPlatform;

import java.util.Collection;
import java.util.Collections;

/**
 * This {@link Plugin} enables to use the basic Rheem {@link Operator}s on the {@link JavaPlatform}.
 */
public class SparkGraphPlugin implements Plugin {

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.GRAPH_MAPPINGS;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {
        return Collections.emptyList();
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Collections.singletonList(SparkPlatform.getInstance());
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }

}
