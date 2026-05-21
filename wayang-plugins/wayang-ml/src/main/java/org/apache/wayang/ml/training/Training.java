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
 * * Unless required by applicable law or agreed to in writing, software * distributed under the License is distributed on an "AS IS" BASIS, * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.ml.training;

import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.plan.wayangplan.ExecutionOperator;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.ml.benchmarks.IMDBJOBenchmark;
import org.apache.logging.log4j.Level;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.ElementaryOperator;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.OperatorBase;
import org.apache.wayang.core.optimizer.cardinality.CardinalityEstimate;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.training.GeneratableJob;
import org.apache.wayang.ml.util.Jobs;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.api.sql.calcite.utils.PrintUtils;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.types.DataSetType;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.reflect.Constructor;
import java.util.Set;
import java.util.List;
import java.util.HashMap;
import java.util.Collection;
import java.time.Instant;
import java.time.Duration;
import java.util.Comparator;
import java.util.stream.Collectors;
import scala.collection.JavaConversions;

public class Training {

    public static String psqlUser = "postgres";
    public static String psqlPassword = "postgres";

    public static void main(String[] args) {
        //trainGeneratables(args[0], args[1], args[2], Integer.valueOf(args[3]), true);
        trainIMDB(args[0], args[1], args[2], args[3], true);
        //trainPadding(args[0], args[1], args[2], true);
    }

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: tpch file path
     * 3: encode to file path (string)
     * 4: job index for the job to run (int)
     * 5: overwrite skipConversions (boolean)
     **/
    public static void trainIMDB(
        String platforms,
        String dataPath,
        String encodePath,
        String query,
        boolean skipConversions
    ) {
        System.out.println("Running job query: " + query);
        try {
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            try {
                String[] jars = new String[]{
                    ReflectionUtils.getDeclaringJar(Training.class),
                    ReflectionUtils.getDeclaringJar(IMDBJOBenchmark.class),
                };

                List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(platforms));
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

                config.setProperty("org.apache.calcite.sql.parser.parserTracing", "true");
                config.setProperty("wayang.calcite.model", calciteModel);
                config.setProperty("wayang.postgres.jdbc.url", "jdbc:postgresql://job:5432/job");
                config.setProperty("wayang.postgres.jdbc.user", psqlUser);
                config.setProperty("wayang.postgres.jdbc.password", psqlPassword);
                config.setProperty("spark.master", "spark://spark-cluster:7077");
                config.setProperty("spark.app.name", "JOB Query");
                config.setProperty("spark.rpc.message.maxSize", "2047");
                config.setProperty("spark.executor.memory", "32g");
                config.setProperty("spark.executor.cores", "6");
                config.setProperty("spark.executor.instances", "1");
                config.setProperty("spark.default.parallelism", "8");
                config.setProperty("spark.driver.maxResultSize", "16g");
                config.setProperty("spark.dynamicAllocation.enabled", "true");
                config.setProperty("wayang.flink.mode.run", "distribution");
                config.setProperty("wayang.flink.parallelism", "1");
                config.setProperty("wayang.flink.master", "flink-cluster");
                config.setProperty("wayang.flink.port", "7071");
                config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
                config.setProperty("wayang.ml.experience.enabled", "false");
                config.setProperty(
                    "wayang.core.optimizer.pruning.strategies",
                    "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy"
                );
                config.setProperty("wayang.core.optimizer.pruning.topk", "1000");

                final MLContext wayangContext = new MLContext(config);
                plugins.stream().forEach(plug -> wayangContext.register(plug));

                System.out.println("Getting plan");
                WayangPlan plan = IMDBJOBenchmark.getWayangPlan(query, config, plugins.toArray(Plugin[]::new), jars);
                //wayangContext.setLogLevel(Level.DEBUG);

                IMDBJOBenchmark.setSources(plan, dataPath);

                Job wayangJob = wayangContext.createJob("", plan, jars);
                ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

                HashMap<Operator, Collection<ExecutionTask>> tree = new HashMap<>();
                Set<ExecutionTask> tasks = exPlan.collectAllTasks();

                Collection<ExecutionTask> roots = tasks.stream()
                    .filter(task -> task.getOperator().isSource())
                    .collect(Collectors.toList());

                for (ExecutionTask task : roots) {
                    ExecutionOperator operator = task.getOperator();

                    if (operator instanceof TextFileSource) {
                        if (operator.getPlatform().getName() == "PostgreSQL") {
                            return;
                        }
                    }
                }

                OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
                TreeNode wayangNode = TreeEncoder.encode(plan);
                TreeNode execNode = TreeEncoder.encode(exPlan, skipConversions).withIdsFrom(wayangNode);
                ExplainUtils.parsePlan(exPlan, true);
                //System.out.println(execNode.toString());

                writer.write(String.format("%s:%s:%d", wayangNode.toStringEncoding(), execNode.toStringEncoding(), 1_000_000));
                writer.newLine();
                writer.flush();
              } catch(WayangException e) {
                  e.printStackTrace();
                  /*
                  writer.write(query);
                  writer.newLine();
                  writer.flush();*/
              }
          } catch(Exception e) {
              e.printStackTrace();
          }
    }

    /*
     * args format:
     * 1: platforms, comma sep. (string)
     * 2: tpch file path
     * 3: encode to file path (string)
     * 4: job index for the job to run (int)
     * 5: overwrite skipConversions (boolean)
     **/
    public static void trainGeneratables(
        String platforms,
        String dataPath,
        String encodePath,
        int index,
        boolean skipConversions
    ) {
        Class<? extends GeneratableJob> job = Jobs.getJob(index);

        try {
            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {platforms, dataPath};
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            String[] jars = new String[]{
                ReflectionUtils.getDeclaringJar(Training.class),
                ReflectionUtils.getDeclaringJar(createdJob.getClass()),
            };

            skipConversions = Boolean.valueOf(skipConversions);
            /*
            * TODO:
            *  - Get DataQuanta's WayangPlan :done:
            *  - Encode WayangPlan and print/store :done:
            *  -- We need to run it once before, so that we can get card estimates :done:
            *  - Call .buildInitialExecutionPlan for the WayangPlan :done:
            *  - Encode the ExecutionPlan and print/store :done:
            *  - Make cardinalities configurable
            */

            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangContext context = builder.getWayangContext();
            Configuration config = context.getConfiguration();
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
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "JOB Query");
            config.setProperty("spark.rpc.message.maxSize", "2047");
            config.setProperty("spark.executor.memory", "32g");
            config.setProperty("spark.executor.cores", "6");
            config.setProperty("spark.executor.instances", "1");
            config.setProperty("spark.default.parallelism", "8");
            config.setProperty("spark.driver.maxResultSize", "16g");
            config.setProperty("spark.dynamicAllocation.enabled", "true");
            config.setProperty("wayang.flink.mode.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "1");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "7071");
            config.setProperty("wayang.flink.rest.client.max-content-length", "200MiB");
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty(
                "wayang.core.optimizer.pruning.strategies",
                "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy"
            );
            config.setProperty("wayang.core.optimizer.pruning.topk", "1000");

            WayangPlan plan = builder.build();

            Job wayangJob = context.createJob("", plan, jars);
            ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();
            OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
            TreeNode wayangNode = TreeEncoder.encode(plan);
            TreeNode execNode = TreeEncoder.encode(exPlan, skipConversions).withIdsFrom(wayangNode);
            //System.out.println(exPlan.toExtensiveString());

            quanta = createdJob.buildPlan(jobArgs);
            builder = quanta.getPlanBuilder();
            context = builder.getWayangContext();
            context.setLogLevel(Level.INFO);
            config = context.getConfiguration();
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + index);
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("wayang.flink.mode.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "8");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "7071");
            config.setProperty("wayang.flink.rest.client.max-content-length", "2000MiB");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + index);
            config.setProperty("spark.executor.memory", "16g");
            //config.setProperty("wayang.core.optimizer.pruning.topk", "100");
            plan = builder.build();
            writer.write(String.format("%s:%s:%d", wayangNode.toStringEncoding(), execNode.toStringEncoding(), 1_000_000));
            writer.newLine();
            writer.flush();
          } catch(Exception e) {
              e.printStackTrace();
          }
    }

    public static void trainPadding(
        String platforms,
        String dataPath,
        String encodePath,
        boolean skipConversions
    ) {
        try {
            String[] jobArgs = {platforms, dataPath};
            FileWriter fw = new FileWriter(encodePath, true);
            BufferedWriter writer = new BufferedWriter(fw);

            String[] jars = new String[]{
                ReflectionUtils.getDeclaringJar(Training.class)
            };

            skipConversions = Boolean.valueOf(skipConversions);
            /*
            * TODO:
            *  - Get DataQuanta's WayangPlan :done:
            *  - Encode WayangPlan and print/store :done:
            *  -- We need to run it once before, so that we can get card estimates :done:
            *  - Call .buildInitialExecutionPlan for the WayangPlan :done:
            *  - Encode the ExecutionPlan and print/store :done:
            *  - Make cardinalities configurable
            */

            Configuration config = new Configuration();
            config.setProperty("wayang.ml.experience.enabled", "false");
            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "Padding Query");
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("spark.executor.cores", "8");
            config.setProperty("wayang.flink.mode.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "8");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "7071");
            config.setProperty("wayang.flink.rest.client.max-content-length", "2000MiB");
            config.setProperty("spark.executor.memory", "16g");
            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(platforms));
            WayangContext context = new WayangContext(config);
            plugins.stream().forEach(plug -> context.register(plug));

            TextFileSource customerText = new TextFileSource(dataPath + "customer.tbl", "UTF-8");

            // Initial identity operator
            MapOperator<String, String> currentOperator = new MapOperator<>(
                    line -> line,
                    String.class,
                    String.class
            );

            // Connect source to the first operator
            customerText.connectTo(0, currentOperator, 0);

            // Chain 249 more identity operators
            for (int i = 1; i < 125; i++) {
                MapOperator<String, String> nextOperator = new MapOperator<>(
                        line -> line,
                        String.class,
                        String.class
                );
                currentOperator.connectTo(0, nextOperator, 0);
                currentOperator = nextOperator; // advance the chain
            }

            // Final sink
            LocalCallbackSink<String> sink = LocalCallbackSink.createStdoutSink(String.class);
            currentOperator.connectTo(0, sink, 0);

            // Build plan
            WayangPlan plan = new WayangPlan(sink);

            Job wayangJob = context.createJob("", plan, jars);
            ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();
            OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());
            TreeNode wayangNode = TreeEncoder.encode(plan);
            TreeNode execNode = TreeEncoder.encode(exPlan, skipConversions).withIdsFrom(wayangNode);
            //System.out.println(exPlan.toExtensiveString());
            writer.write(String.format("%s:%s:%d", wayangNode.toStringEncoding(), execNode.toStringEncoding(), 1_000_000));
            writer.newLine();
            writer.flush();
          } catch(Exception e) {
              e.printStackTrace();
          }
    }
}
