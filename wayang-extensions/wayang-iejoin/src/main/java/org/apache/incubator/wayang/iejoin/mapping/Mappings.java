package org.apache.incubator.wayang.iejoin.mapping;

import org.apache.incubator.wayang.core.mapping.Mapping;
import org.apache.incubator.wayang.iejoin.mapping.spark.IEJoinMapping;
import org.apache.incubator.wayang.iejoin.mapping.spark.IESelfJoinMapping;
import org.apache.incubator.wayang.iejoin.operators.IEJoinOperator;
import org.apache.incubator.wayang.java.platform.JavaPlatform;
import org.apache.incubator.wayang.spark.platform.SparkPlatform;

import java.util.Arrays;
import java.util.Collection;

/**
 * {@link Mapping}s for the {@link IEJoinOperator}.
 */
public class Mappings {

    /**
     * {@link Mapping}s towards the {@link JavaPlatform}.
     */
    public static Collection<Mapping> javaMappings = Arrays.asList(
            new org.apache.incubator.wayang.iejoin.mapping.java.IEJoinMapping(), new org.apache.incubator.wayang.iejoin.mapping.java.IESelfJoinMapping()
    );

    /**
     * {@link Mapping}s towards the {@link SparkPlatform}.
     */
    public static Collection<Mapping> sparkMappings = Arrays.asList(
            new IEJoinMapping(), new IESelfJoinMapping()
    );

}
