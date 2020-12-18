package org.apache.incubator.wayang.iejoin;

import org.apache.incubator.wayang.core.api.Configuration;
import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.core.optimizer.channels.ChannelConversion;
import org.apache.incubator.wayang.core.platform.Platform;
import org.apache.incubator.wayang.core.plugin.Plugin;
import org.apache.incubator.wayang.iejoin.mapping.Mappings;
import org.apache.incubator.wayang.iejoin.operators.IEJoinOperator;
import org.apache.incubator.wayang.iejoin.operators.IESelfJoinOperator;
import org.apache.incubator.wayang.java.Java;
import org.apache.incubator.wayang.java.platform.JavaPlatform;
import org.apache.incubator.wayang.spark.Spark;
import org.apache.incubator.wayang.spark.platform.SparkPlatform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Provides {@link Plugin}s that enable usage of the {@link IEJoinOperator} and the {@link IESelfJoinOperator}.
 */
public class IEJoin {

    /**
     * Enables use with the {@link JavaPlatform} and {@link SparkPlatform}.
     */
    private static final Plugin PLUGIN = new Plugin() {

        @Override
        public Collection<Platform> getRequiredPlatforms() {
            return Arrays.asList(Java.platform(), Spark.platform());
        }

        @Override
        public Collection<Mapping> getMappings() {
            Collection<Mapping> mappings = new ArrayList<>();
            mappings.addAll(Mappings.javaMappings);
            mappings.addAll(Mappings.sparkMappings);
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
     * Retrieve a {@link Plugin} to use {@link IEJoinOperator} and {@link IESelfJoinOperator} on the
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
            return Mappings.javaMappings;
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
     * Retrieve a {@link Plugin} to use {@link IEJoinOperator} and {@link IESelfJoinOperator} on the
     * and {@link JavaPlatform}.
     *
     * @return the {@link Plugin}
     */
    public static Plugin javaPlugin() {
        return JAVA_PLUGIN;
    }

    /**
     * Enables use with the {@link SparkPlatform}.
     */
    public static final Plugin SPARK_PLUGIN = new Plugin() {

        @Override
        public Collection<Platform> getRequiredPlatforms() {
            return Collections.singleton(Spark.platform());
        }

        @Override
        public Collection<Mapping> getMappings() {
            return Mappings.sparkMappings;
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
     * Retrieve a {@link Plugin} to use {@link IEJoinOperator} and {@link IESelfJoinOperator} on the
     * and {@link SparkPlatform}.
     *
     * @return the {@link Plugin}
     */
    public static Plugin sparkPlugin() {
        return SPARK_PLUGIN;
    }

}
