package io.rheem.rheem.iejoin.mapping;

import io.rheem.rheem.core.mapping.Mapping;
import io.rheem.rheem.iejoin.mapping.spark.IEJoinMapping;
import io.rheem.rheem.iejoin.mapping.spark.IESelfJoinMapping;
import io.rheem.rheem.iejoin.operators.IEJoinOperator;
import io.rheem.rheem.java.platform.JavaPlatform;
import io.rheem.rheem.spark.platform.SparkPlatform;

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
            new io.rheem.rheem.iejoin.mapping.java.IEJoinMapping(), new io.rheem.rheem.iejoin.mapping.java.IESelfJoinMapping()
    );

    /**
     * {@link Mapping}s towards the {@link SparkPlatform}.
     */
    public static Collection<Mapping> sparkMappings = Arrays.asList(
            new IEJoinMapping(), new IESelfJoinMapping()
    );

}
