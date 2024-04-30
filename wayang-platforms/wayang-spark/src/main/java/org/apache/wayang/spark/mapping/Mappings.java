/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.spark.mapping;

import org.apache.wayang.core.mapping.Mapping;
import org.apache.wayang.spark.mapping.graph.PageRankMapping;
import org.apache.wayang.spark.mapping.ml.*;

import java.util.Arrays;
import java.util.Collection;

/**
 * Register for {@link Mapping}s for this platform.
 */
public class Mappings {

    public static Collection<Mapping> BASIC_MAPPINGS = Arrays.asList(
            new TextFileSourceMapping(),
            new TextFileSinkMapping(),
            new ObjectFileSinkMapping(),
            new ObjectFileSourceMapping(),
            new MapMapping(),
            new MapPartitionsMapping(),
            new ReduceByMapping(),
            new CollectionSourceMapping(),
            new LocalCallbackSinkMapping(),
            new GlobalReduceMapping(),
            new MaterializedGroupByMapping(),
            new GlobalMaterializedGroupMapping(),
            new FlatMapMapping(),
            new CountMapping(),
            new DistinctMapping(),
            new SortMapping(),
            new FilterMapping(),
            new UnionAllMapping(),
            new IntersectMapping(),
            new CartesianMapping(),
            new JoinMapping(),
            new CoGroupMapping(),
            new LoopMapping(),
            new DoWhileMapping(),
            new RepeatMapping(),
            new SampleMapping(),
            new ZipWithIdMapping()
    );

    public static Collection<Mapping> GRAPH_MAPPINGS = Arrays.asList(
            new PageRankMapping()
    );

    public static Collection<Mapping> ML_MAPPINGS = Arrays.asList(
            new KMeansMapping(),
            new LinearRegressionMapping(),
            new DecisionTreeClassificationMapping(),
            new ModelTransformMapping(),
            new PredictMapping()
    );

}
