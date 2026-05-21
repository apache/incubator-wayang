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

package org.apache.wayang.ml.training;

import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.Jobs;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.api.sql.calcite.rel.WayangJoin;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.ml.util.CardinalitySampler;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.logging.log4j.Level;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.wayang.ml.benchmarks.IMDBJOBenchmark;
import org.apache.wayang.ml.benchmarks.JOBenchmark;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.time.Instant;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.Collection;
import java.util.Collections;
import scala.collection.JavaConversions;
import java.util.List;

public class Encode {

    public static String psqlUser = "postgres";
    public static String psqlPassword = "postgres";

    public static void main(String[] args) {
        List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
        Configuration config = new Configuration();

        final String calciteModel = "{\n" +
                "    \"version\": \"1.0\",\n" +
                "    \"defaultSchema\": \"wayang\",\n" +
                "    \"schemas\": [\n" +
                "        {\n" +
                "            \"name\": \"postgres\",\n" +
                "            \"type\": \"custom\",\n" +
                "            \"factory\": \"org.apache.wayang.api.sql.calcite.jdbc.JdbcSchema$Factory\",\n" +
                "            \"operand\": {\n" +
                "                \"jdbcDriver\": \"org.postgresql.Driver\",\n" +
                "                \"jdbcUrl\": \"jdbc:postgresql://job:5432/job\",\n" +
                "                \"jdbcUser\": \"" + psqlUser + "\",\n" +
                "                \"jdbcPassword\": \"" + psqlPassword + "\"\n" +
                "            }\n" +
                "        }\n" +
                "    ]\n" +
                "}";

        config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));
        config.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
        config.setProperty("wayang.calcite.model", calciteModel);
        config.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
        config.setProperty("wayang.postgres.jdbc.user", psqlUser);
        config.setProperty("wayang.postgres.jdbc.password", psqlPassword);

        config.setProperty("spark.master", "spark://spark-cluster:7077");
        config.setProperty("spark.app.name", "JOB Query");
        config.setProperty("spark.rpc.message.maxSize", "2047");
        config.setProperty("spark.executor.memory", "42g");
        config.setProperty("spark.executor.cores", "8");
        config.setProperty("spark.executor.instances", "1");
        config.setProperty("spark.default.parallelism", "2");
        config.setProperty("spark.driver.maxResultSize", "16g");
        config.setProperty("spark.dynamicAllocation.enabled", "true");
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

        String[] jars = ArrayUtils.addAll(
            ReflectionUtils.getAllJars(Encode.class),
            ReflectionUtils.getLibs(Encode.class)
        );

        jars = ArrayUtils.addAll(
            jars,
            ReflectionUtils.getAllJars(org.apache.calcite.rel.externalize.RelJson.class)
        );


        //encodeGeneratables(args[0], args[1], args[2], Integer.valueOf(args[3]), true);
        encodeJob(plugins, args[1], args[2], args[3], config, jars);
    }

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: tpch file path
     * 3: encode to file path (string)
     * 4: job index for the job to run (int)
     * 5: overwrite cardinalities (boolean)
     **/
    public static void encodeGeneratables(String platforms, String dataPath, String encodePath, int index, boolean rewriteCardinalities) {
        int counter = 0;
        Class<? extends GeneratableJob> job = Jobs.getJob(index);

        try {
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {platforms, dataPath};

            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangContext context = builder.getWayangContext();
            Configuration config = context.getConfiguration();
            WayangPlan plan = builder.build();
            Job wayangJob = context.createJob("", plan, "");
            context.setLogLevel(Level.ERROR);
            buildPlanImplementations(wayangJob, plan, context, writer);
                //CardinalitySampler.readFromFile(path);
        }catch(Exception e) {
            e.printStackTrace();
        }
    }

    public static void encodeJob(
            List<Plugin> plugins,
            String dataPath,
            String encodePath,
            String queryPath,
            Configuration config,
            String... jars
    ) {
        try {
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            WayangPlan plan = IMDBJOBenchmark.getWayangPlan(queryPath, config, plugins.toArray(Plugin[]::new), jars);
            IMDBJOBenchmark.setSources(plan, dataPath);

            WayangContext context = IMDBJOBenchmark.sqlContext;
            Job wayangJob = context.createJob("", plan, "");
            context.setLogLevel(Level.ERROR);
            buildPlanImplementations(wayangJob, plan, context, writer);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void buildPlanImplementations(Job job, WayangPlan wayangPlan, WayangContext wayangContext, BufferedWriter writer) {
        ExecutionPlan baseplan = job.buildInitialExecutionPlan();
        OneHotMappings.setOptimizationContext(job.getOptimizationContext());
        TreeNode wayangNode = TreeEncoder.encode(wayangPlan);

        Experiment experiment = new Experiment("wayang-ml-test", new Subject("Wayang", "0.1"));
        StopWatch stopWatch = new StopWatch(experiment);
        TimeMeasurement optimizationRound = stopWatch.getOrCreateRound("optimization");
        final PlanEnumerator planEnumerator = new PlanEnumerator(wayangPlan, job.getOptimizationContext());

        final TimeMeasurement enumerateMeasurment = optimizationRound.start("Create Initial Execution Plan", "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        Collection<PlanImplementation> planImplementations = comprehensiveEnumeration.getPlanImplementations();

        for (PlanImplementation planImplementation : planImplementations) {
            try {
                TreeNode planImplNode = TreeEncoder.encode(planImplementation).withIdsFrom(wayangNode);
                writer.write(String.format("%s:%s:%.0f", wayangNode.toStringEncoding(), planImplNode.toStringEncoding(), planImplementation.getSquashedCostEstimate()));
                writer.newLine();
                writer.flush();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        planImplementations = Collections.emptyList();
    }
}
