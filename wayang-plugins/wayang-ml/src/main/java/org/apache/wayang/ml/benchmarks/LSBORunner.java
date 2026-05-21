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
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;

import org.apache.wayang.api.python.executor.ProcessFeeder;
import org.apache.wayang.api.python.executor.ProcessReceiver;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.LSBO;
import org.apache.wayang.ml.training.TPCH;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.apps.tpch.queries.Query3;
import org.apache.wayang.apps.tpch.queries.Query5;
import org.apache.wayang.apps.tpch.queries.Query6;
import org.apache.wayang.apps.tpch.queries.Query10;
import org.apache.wayang.apps.tpch.queries.Query12;
import org.apache.wayang.apps.tpch.queries.Query14;
import org.apache.wayang.apps.tpch.queries.Query19;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.ml.training.GeneratableJob;
import org.apache.wayang.ml.benchmarks.IMDBJOBenchmark;
import org.apache.wayang.ml.benchmarks.JOBenchmark;
import org.apache.wayang.ml.util.Jobs;
import org.apache.commons.lang3.ArrayUtils;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.time.Duration;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;

import scala.collection.Seq;
import scala.collection.JavaConversions;
import com.google.protobuf.ByteString;

/**
 * TODO:
 *  - Move this to a class so that LSBO is a utility function
 *  -- Takes wayang plan as input
 *  -- Encodes it and sends it to python
 *  -- Receives set of encoded strings from latent space
 *  -- Executes each of those on the original plan
 */
public class LSBORunner {

    public static String psqlUser = "ucloud";
    public static String psqlPassword = "ucloud";

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

        // Executor memory (on 48GB nodes)
        config.setProperty("spark.executor.memory", "36g");
        config.setProperty("spark.executor.memoryOverhead", "4g");
        config.setProperty("spark.executor.cores", "8");

        // Driver memory (on 48GB master node)
        config.setProperty("spark.driver.memory", "24g");
        config.setProperty("spark.driver.memoryOverhead", "2g");
        config.setProperty("spark.driver.maxResultSize", "8g");

        // Dynamic allocation (drop spark.executor.instances)
        config.setProperty("spark.dynamicAllocation.enabled", "true");
        config.setProperty("spark.dynamicAllocation.minExecutors", "2");
        config.setProperty("spark.dynamicAllocation.maxExecutors", "2");
        config.setProperty("spark.dynamicAllocation.initialExecutors", "2");
        config.setProperty("spark.shuffle.service.enabled", "true");

        // Parallelism
        config.setProperty("spark.default.parallelism", "16"); // 2 executors * 8 cores

        config.setProperty("wayang.flink.mode.run", "distribution");
        config.setProperty("wayang.flink.parallelism", "1");
        config.setProperty("wayang.flink.master", "flink-cluster");
        config.setProperty("wayang.flink.port", "7071");
        config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
        config.setProperty("wayang.flink.collect.path", "file:///work/lsbo-paper/data/flink-data");
        //config.setProperty("wayang.flink.collect.path", "file:///tmp/flink-data");
        config.setProperty("wayang.ml.experience.enabled", "false");
        /*
        config.setProperty(
            "wayang.core.optimizer.pruning.strategies",
            "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy"
        );
        config.setProperty("wayang.core.optimizer.pruning.topk", "10000");*/

        String[] jars = ArrayUtils.addAll(
            ReflectionUtils.getAllJars(LSBORunner.class),
            ReflectionUtils.getLibs(LSBORunner.class)
        );

        jars = ArrayUtils.addAll(
            jars,
            ReflectionUtils.getAllJars(org.apache.calcite.rel.externalize.RelJson.class)
        );


        /*
        HashMap<String, WayangPlan> plans = TPCH.createPlans(args[1]);
        WayangPlan plan = plans.get("query" + args[2]);*/

        try {
            //WayangPlan plan = getTPCHPlan(args[0], args[1], Integer.parseInt(args[2]));
            WayangPlan plan = getJOBPlan(plugins, args[1], config, args[2], jars);

            //Set sink to be on Java
            /*
            ((LinkedList<Operator>) plan.getSinks())
                .get(0)
                .addTargetPlatform(Java.platform());*/

            LSBO.process(plan, config, plugins, jars);
        } catch(Exception e) {
            System.out.println(e.getMessage());
        }

    }

    private static WayangPlan getJOBPlan(List<Plugin> plugins, String dataPath, Configuration config, String queryPath, String[] jars) {
        try {
            WayangPlan plan = IMDBJOBenchmark.getWayangPlan(queryPath, config, plugins.toArray(Plugin[]::new), jars);
            IMDBJOBenchmark.setSources(plan, dataPath);

            return plan;
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }

    private static WayangPlan getTPCHPlan(String platforms, String dataPath, int query) {
        try {
            Class<? extends GeneratableJob> job = Jobs.getJob(query);

            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {platforms, dataPath};
            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangPlan plan = builder.build();

            return plan;
        } catch (Exception e) {
            e.printStackTrace();

            return null;
        }
    }
}
