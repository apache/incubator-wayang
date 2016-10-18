package org.qcri.rheem.iejoin.mapping;

import org.qcri.rheem.core.mapping.Mapping;
import org.qcri.rheem.iejoin.mapping.spark.IEJoinMapping;
import org.qcri.rheem.iejoin.mapping.spark.IESelfJoinMapping;
import org.qcri.rheem.iejoin.operators.IEJoinOperator;
import org.qcri.rheem.java.platform.JavaPlatform;
import org.qcri.rheem.spark.platform.SparkPlatform;

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
            new org.qcri.rheem.iejoin.mapping.java.IEJoinMapping(), new org.qcri.rheem.iejoin.mapping.java.IESelfJoinMapping()
    );

    /**
     * {@link Mapping}s towards the {@link SparkPlatform}.
     */
    public static Collection<Mapping> sparkMappings = Arrays.asList(
            new IEJoinMapping(), new IESelfJoinMapping()
    );

}
