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

package org.apache.wayang.ml.benchmarks;

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.spark.platform.SparkPlatform;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.ml.costs.MLCost;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.logging.log4j.Level;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.apps.imdb.data.*;
import org.apache.commons.lang3.ArrayUtils;

import scala.collection.Seq;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * Example Apache Wayang (incubating) App that does a word count -- the Hello World of Map/Reduce-like systems.
 */
public class JOBLightQuery1 {

    /**
     * Creates the {@link WayangPlan} for the word count app.
     *
     * @param inputFileUrl the file whose words should be counted
     */
    public static WayangPlan createWayangPlan(String inputFilesUrl, Collection<Tuple2<Tuple2<Title, MovieCompanies>, MovieInfo>> collector) throws URISyntaxException, IOException {
        // TextFileSources
        TextFileSource titleSource = new TextFileSource(inputFilesUrl + "/title.csv");
        titleSource.setName("Load title file");

        TextFileSource movieCompaniesSource = new TextFileSource(inputFilesUrl + "/movie_companies.csv");
        movieCompaniesSource.setName("Load movie_companies file");

        TextFileSource movieInfoIdxSource = new TextFileSource(inputFilesUrl + "/movie_info_idx.csv");
        movieInfoIdxSource.setName("Load movie_info_idx file");

        // Parsing
        MapOperator<String, Title> titleParser = new MapOperator<>(
                (line) -> Title.parseCsv(line), String.class, Title.class
        );

        MapOperator<String, MovieCompanies> mcParser = new MapOperator<>(
                (line) -> MovieCompanies.parseCsv(line), String.class, MovieCompanies.class
        );

        MapOperator<String, MovieInfo> miIdxParser = new MapOperator<>(
                (line) -> MovieInfo.parseCsv(line), String.class, MovieInfo.class
        );

        // Filters pushed down
        FilterOperator<MovieInfo> miIdxFilter = new FilterOperator<>(
                (tuple) -> tuple.infoTypeId() == 112,
                MovieInfo.class
        );

        FilterOperator<MovieCompanies> mcFilter = new FilterOperator<>(
                (tuple) -> tuple.companyTypeId() == 2,
                MovieCompanies.class
        );


        // Joins
        JoinOperator<Title, MovieCompanies, Integer> tMcJoin = new JoinOperator<>(
            (title) -> title.id(),
            (mc) -> mc.movieId(),
            Title.class,
            MovieCompanies.class,
            Integer.class
        );

        JoinOperator<Tuple2<Title, MovieCompanies>, MovieInfo, Integer> tMcMiIdxJoin = new JoinOperator<>(
            (tuple) -> tuple.field0.id(),
            (miIdx) -> miIdx.movieId(),
            ReflectionUtils.specify(Tuple2.class),
            MovieInfo.class,
            Integer.class
        );

        // Sink
        LocalCallbackSink<Tuple2<Tuple2<Title, MovieCompanies>, MovieInfo>> sink = LocalCallbackSink.createCollectingSink(
            collector,
            ReflectionUtils.specify(Tuple2.class)
        );


        // Operator connections
        titleSource.connectTo(0, titleParser, 0);
        movieCompaniesSource.connectTo(0, mcParser, 0);
        movieInfoIdxSource.connectTo(0, miIdxParser, 0);

        mcParser.connectTo(0, mcFilter, 0);
        miIdxParser.connectTo(0, miIdxFilter, 0);

        titleParser.connectTo(0, tMcJoin, 0);
        mcFilter.connectTo(0, tMcJoin, 1);

        tMcJoin.connectTo(0, tMcMiIdxJoin, 0);
        miIdxFilter.connectTo(0, tMcMiIdxJoin, 1);

        tMcMiIdxJoin.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        try {
            if (args.length == 0) {
                System.err.print("Usage: <platform1>[,<platform2>]* <input file URL>");
                System.exit(1);
            }

            List<Tuple2<Tuple2<Title, MovieCompanies>, MovieInfo>> collector = new LinkedList<>();
            WayangPlan wayangPlan = createWayangPlan(args[1], collector);

            Configuration config = new Configuration();
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "JOB Query");
            config.setProperty("spark.rpc.message.maxSize", "2047");
            config.setProperty("spark.executor.memory", "42g");
            config.setProperty("spark.executor.cores", "4");
            config.setProperty("spark.executor.instances", "2");
            config.setProperty("spark.default.parallelism", "8");
            config.setProperty("spark.driver.maxResultSize", "16g");
            config.setProperty("spark.shuffle.service.enabled", "true");
            config.setProperty("spark.dynamicAllocation.enabled", "true");
            config.setProperty("spark.dynamicAllocation.minExecutors", "2");
            config.setProperty("wayang.flink.mode.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "1");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "7071");
            config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
            config.setProperty("wayang.flink.collect.path", "file:///work/lsbo-paper/data/flink-data");
            //config.setProperty("wayang.flink.collect.path", "file:///tmp/flink-data");
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty(
                "wayang.core.optimizer.pruning.strategies",
                "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy"
            );
            config.setProperty("wayang.core.optimizer.pruning.topk", "100");

            final WayangContext wayangContext = new WayangContext(config);

            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            plugins.stream().forEach(plug -> wayangContext.register(plug));

            String[] jars = ArrayUtils.addAll(
                ReflectionUtils.getAllJars(JOBLightQuery1.class),
                ReflectionUtils.getLibs(JOBLightQuery1.class)
            );

            wayangContext.execute(wayangPlan, jars);

            System.out.printf("Result size: %d\n", collector.size());
        } catch (Exception e) {
            System.err.println("App failed.");
            e.printStackTrace();
            System.exit(4);
        }
    }

}
