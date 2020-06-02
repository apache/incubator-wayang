package org.qcri.rheem.spark.plugin;

import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.core.optimizer.channels.ChannelConversion;
import org.qcri.rheem.core.plan.rheemplan.Operator;
import org.qcri.rheem.core.platform.Platform;
import org.qcri.rheem.core.plugin.Plugin;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.mapping.Mappings;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
