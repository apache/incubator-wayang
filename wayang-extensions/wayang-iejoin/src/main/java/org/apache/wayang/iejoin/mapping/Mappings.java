package org.apache.wayang.iejoin.mapping;

import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.iejoin.mapping.spark.IEJoinMapping;
import org.apache.wayang.iejoin.mapping.spark.IESelfJoinMapping;
import org.apache.wayang.iejoin.operators.IEJoinOperator;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.platform.SparkPlatform;

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
            new org.apache.wayang.iejoin.mapping.java.IEJoinMapping(), new org.apache.wayang.iejoin.mapping.java.IESelfJoinMapping()
    );

    /**
     * {@link Mapping}s towards the {@link SparkPlatform}.
     */
    public static Collection<Mapping> sparkMappings = Arrays.asList(
            new IEJoinMapping(), new IESelfJoinMapping()
    );

}
