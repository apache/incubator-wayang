package io.rheem.rheem.iejoin;

import io.rheem.rheem.core.api.Configuration;
import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.core.optimizer.channels.ChannelConversion;
import io.rheem.rheem.core.platform.Platform;
import io.rheem.rheem.core.plugin.Plugin;
import io.rheem.rheem.iejoin.mapping.Mappings;
import io.rheem.rheem.iejoin.operators.IEJoinOperator;
import io.rheem.rheem.iejoin.operators.IESelfJoinOperator;
import io.rheem.rheem.java.Java;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.spark.Spark;
import io.rheem.rheem.spark.platform.SparkPlatform;

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
