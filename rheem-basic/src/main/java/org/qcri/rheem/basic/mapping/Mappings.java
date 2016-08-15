package org.qcri.rheem.basic.mapping;

import org.qcri.rheem.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for the components provided in the basic plugin.
 */
public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new ReduceByMapping(),
            new MaterializedGroupByMapping(),
            new GlobalReduceMapping()
    );

    public static Collection<Mapping> GRAPH_MAPPINGS = Arrays.asList(
            new PageRankMapping()
    );

}
