package org.apache.wayang.agoraeo.mappings;

import org.apache.wayang.agoraeo.mappings.java.Sen2CorWrapperJavaMapping;
import org.apache.wayang.agoraeo.mappings.java.SentinelSourceJavaMapping;
import org.apache.wayang.agoraeo.mappings.spark.Sen2CorWrapperSparkMapping;
import org.apache.wayang.agoraeo.mappings.spark.SentinelSourceSparkMapping;
import org.apache.wayang.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
//            new SentinelSourceJavaMapping(),
//            new Sen2CorWrapperJavaMapping(),
            new SentinelSourceSparkMapping(),
            new Sen2CorWrapperSparkMapping()
    );

}
