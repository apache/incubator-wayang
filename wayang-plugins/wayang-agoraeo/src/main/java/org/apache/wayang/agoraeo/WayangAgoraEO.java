package org.apache.wayang.agoraeo;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.wayang.core.platform.Platform;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.wayang.agoraeo.mappings.Mappings;

public class WayangAgoraEO {

    private static final Plugin PLUGIN = new Plugin() {

        @Override
        public Collection<Platform> getRequiredPlatforms() {
            return Arrays.asList(Java.platform(), Spark.platform());
        }

        @Override
        public Collection<Mapping> getMappings() {
            Collection<Mapping> mappings = new ArrayList<>();
            mappings.addAll(Mappings.BASIC_MAPPINGS);
//            mappings.addAll(Mappings.sparkMappings);
            return mappings;
        }

        @Override
        public Collection<ChannelConversion> getChannelConversions() {
            return Collections.emptyList();
        }

        @Override
        public void setProperties(Configuration configuration) {
        }
    };

    /**
     * Retrieve a {@link Plugin} to use {@link WayangAgoraEO} on the
     * {@link JavaPlatform} and {@link SparkPlatform}.
     *
     * @return the {@link Plugin}
     */
    public static Plugin plugin() {
        return PLUGIN;
    }

    /**
     * Enables use with the {@link JavaPlatform}.
     */
    private static final Plugin JAVA_PLUGIN = new Plugin() {

        @Override
        public Collection<Platform> getRequiredPlatforms() {
            return Collections.singleton(Java.platform());
        }

        @Override
        public Collection<Mapping> getMappings() {
            return Mappings.BASIC_MAPPINGS;
        }

        @Override
        public Collection<ChannelConversion> getChannelConversions() {
            return Collections.emptyList();
        }

        @Override
        public void setProperties(Configuration configuration) {
        }
    };

    /**
     * Retrieve a {@link Plugin} to use {@link WayangAgoraEO} on the
     * {@link JavaPlatform}.
     *
     * @return the {@link Plugin}
     */
    public static Plugin javaPlugin() {
        return JAVA_PLUGIN;
    }

    /**
     * Retrieve a {@link Plugin} to use {@link WayangIterators} on the
     * {@link SparkPlatform}.
     */
//    public static final Plugin SPARK_PLUGIN = new Plugin() {
//
//        @Override
//        public Collection<Platform> getRequiredPlatforms() {
//            return Collections.singleton(Spark.platform());
//        }
//
//        @Override
//        public Collection<Mapping> getMappings() {
//            return Mappings.sparkMappings;
//        }
//
//        @Override
//        public Collection<ChannelConversion> getChannelConversions() {
//            return Collections.emptyList();
//        }
//
//        @Override
//        public void setProperties(Configuration configuration) {
//        }
//    };
//
//
//    /**
//     * Retrieve a {@link Plugin} to use {@link WayangAgoraEO} on the
//     * {@link SparkPlatform}.
//     *
//     * @return the {@link Plugin}
//     */
//    public static Plugin sparkPlugin() {
//        return SPARK_PLUGIN;
//    }
}
