package org.qcri.rheem.flink.mapping;

import org.qcri.rheem.core.mapping.Mapping;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for {@link Mapping}s for this platform.
 */
public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new CartesianMapping(),
            new CoGroupMapping(),
            new CollectionSourceMapping(),
            new CountMapping(),
            new DistinctMapping(),
            new DoWhileMapping(),
            new FilterMapping(),
            new FlatMapMapping(),
            new GlobalMaterializedGroupMapping(),
            new GlobalReduceMapping(),
            new GroupByMapping(),
            new IntersectMapping(),
            new JoinMapping(),
            new LocalCallbackSinkMapping(),
            new LoopMapping(),
            new MapMapping(),
            new MapPartitionsMapping(),
            new MaterializedGroupByMapping(),
            new PageRankMapping(),
            new ReduceByMapping(),
            new RepeatMapping(),
            new SampleMapping(),
            new SortMapping(),
            new TextFileSinkMapping(),
            new TextFileSourceMapping(),
            new UnionAllMapping(),
            new ZipWithIdMapping()
    );

}


