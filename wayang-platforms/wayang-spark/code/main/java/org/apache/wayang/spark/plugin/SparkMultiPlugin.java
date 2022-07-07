package org.apache.wayang.spark.plugin;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.channels.ChannelConversions;
import org.apache.wayang.spark.mapping.Mappings;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;

public class SparkMultiPlugin  implements Plugin {

    String name;
    String config;

    public SparkMultiPlugin(String name, String config){
        this.name = name;
        this.config = config;
    }

    @Override
    public Collection<Mapping> getMappings() {
        return Mappings.getBasicMappings();
//        return Mappings.BASIC_MAPPINGS;
    }

    @Override
    public Collection<ChannelConversion> getChannelConversions() {

        return ChannelConversions.ALL;
    }

    @Override
    public Collection<Platform> getRequiredPlatforms() {
        return Arrays.asList(SparkPlatform.getInstance(this.name, this.config), JavaPlatform.getInstance());
    }

    @Override
    public void setProperties(Configuration configuration) {
        // Nothing to do, because we already configured the properties in #configureDefaults(...).
    }
}
