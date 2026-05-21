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
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.basic.operators.JoinOperator;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.postgres.Postgres;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;
import org.apache.logging.log4j.Level;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.TableSource;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.api.sql.context.SqlContext;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.api.DataQuanta;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.ml.encoding.OneHotMappings;

import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import scala.collection.Seq;
import scala.collection.JavaConversions;
import java.util.Collection;
import java.io.BufferedWriter;
import java.io.FileWriter;

public class Inference {

    /**
     * 0: platforms
     * 1: Data directory
     * 2: Directory to write timings to
     * 3: query path
     * 4: model type
     * 5: model path
     * 6: experience path
     */
    public static String psqlUser = "postgres";
    public static String psqlPassword = "postgres";

    public static void main(String[] args) {
        try {
            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            Configuration config = new Configuration();
            String modelType = "";

            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "JOB Query");
            config.setProperty("spark.rpc.message.maxSize", "2047");
            config.setProperty("spark.executor.memory", "32g");
            config.setProperty("spark.executor.cores", "4");
            config.setProperty("spark.executor.instances", "1");
            config.setProperty("spark.default.parallelism", "8");
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
            config.setProperty("wayang.core.optimizer.pruning.topk", "1000");

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

            config.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
            config.setProperty("wayang.calcite.model", calciteModel);
            config.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
            config.setProperty("wayang.postgres.jdbc.user", psqlUser);
            config.setProperty("wayang.postgres.jdbc.password", psqlPassword);

            if (args.length > 4) {
                modelType = args[4];
            }

            if (args.length > 6) {
                Inference.setMLModel(config, modelType, args[5], args[6]);
            }

            // Take the query name
            String fileName = args[3].substring(args[3].lastIndexOf("/") + 1);
            String queryName = fileName.substring(0, fileName.lastIndexOf("."));

            String executionTimeFile = args[2] + "query-executions-" + queryName;
            String optimizationTimeFile = args[2] + "query-optimizations-" + queryName;

            if (!"".equals(modelType)) {
                executionTimeFile += "-" + modelType;
                optimizationTimeFile += "-" + modelType;
            }

            config.setProperty(
                "wayang.ml.executions.file",
                executionTimeFile + ".txt"
            );

            config.setProperty(
                "wayang.ml.optimizations.file",
                optimizationTimeFile + ".txt"
            );

            final MLContext wayangContext = new MLContext(config);
            plugins.stream().forEach(plug -> wayangContext.register(plug));

            String[] jars = ArrayUtils.addAll(
                ReflectionUtils.getAllJars(JOBenchmark.class),
                ReflectionUtils.getLibs(JOBenchmark.class)
            );

            jars = ArrayUtils.addAll(
                jars,
                ReflectionUtils.getAllJars(org.apache.calcite.runtime.SqlFunctions.class)
            );

            try {
                WayangPlan plan = IMDBJOBenchmark.getWayangPlan(args[3], config, plugins.toArray(Plugin[]::new), jars);

                IMDBJOBenchmark.setSources(plan, args[1]);

                ExecutionPlan executionPlan;

                if (!"vae".equals(modelType) && !"bvae".equals(modelType)) {
                    executionPlan = wayangContext.buildInitialExecutionPlan("", plan, jars);

                    Job job = wayangContext.createJob("", plan, jars);
                    Configuration jobConfig = job.getConfiguration();
                    job.estimateKeyFigures();
                    OneHotMappings.setOptimizationContext(job.getOptimizationContext());
                    OneHotMappings.encodeIds = false;
                } else {
                    OneHotMappings.encodeIds = true;
                    executionPlan = wayangContext.buildWithVAE(plan, jars);
                    OneHotMappings.encodeIds = false;
                }

                ExplainUtils.parsePlan(executionPlan, true);
                //System.out.println(TreeEncoder.encode(executionPlan, true).toStringEncoding());
                System.out.println("DONE");
            } catch (SqlParseException sqlE) {
                sqlE.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setMLModel(Configuration config, String modelType, String path, String experiencePath) {
        config.setProperty(
            "wayang.ml.model.file",
            path
        );

        switch(modelType) {
            case "cost":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-cost.txt");

                config.setCostModel(new PointwiseCost());

                break;
            case "pairwise":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-pairwise.txt");
                config.setCostModel(new PairwiseCost());

                break;
            case "bvae":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-bvae.txt");

                break;
            case "vae":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-vae.txt");

                break;
            default:
                break;
        }

    }
}
