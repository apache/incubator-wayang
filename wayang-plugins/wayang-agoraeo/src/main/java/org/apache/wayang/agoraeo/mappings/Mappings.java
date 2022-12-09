package org.apache.wayang.agoraeo.mappings;

import org.apache.wayang.agoraeo.mappings.java.SentinelSourceMapping;
import org.apache.wayang.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new SentinelSourceMapping()
    );

}
