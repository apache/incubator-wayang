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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.Level;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;

import scala.collection.JavaConversions;

public class JOBenchmark {

    /**
     * 0: platforms
     * 1: Data directory
     * 2: Directory to write timings to
     * 3: query path
     * 4: model type
     * 5: model path
     * 6: experience path
     */
    public static String psqlUser = "ucloud";
    public static String psqlPassword = "ucloud";

    public static void main(String[] args) {
        try {
            List<Plugin> pluginsSeq = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            ArrayList<Plugin> plugins = new ArrayList<>(pluginsSeq);
            plugins.add(Java.testSuitePlugin());
            plugins.add(Java.channelConversionPlugin());

            Configuration config = new Configuration();
            String modelType = "";

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
                JOBenchmark.setMLModel(config, modelType, args[5], args[6]);
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

                //ExplainUtils.parsePlan(plan, true);

                //Set sink to be on Java
                //((LinkedList<Operator> )plan.getSinks()).get(0).addTargetPlatform(Java.platform());
                //
                /*
                FileWriter fw = new FileWriter(
                    "/var/www/html/data/benchmarks/operators.txt",
                    true
                );
                BufferedWriter writer = new BufferedWriter(fw);


                System.out.println("Operators: " + operators.size());

                writer.write(args[3] + ": " + operators.size());
                writer.newLine();
                writer.flush();
                writer.close();&*/
                wayangContext.setLogLevel(Level.DEBUG);

                if (!"vae".equals(modelType) && !"bvae".equals(modelType)) {
                    System.out.println("Executing query " + args[3]);
                    wayangContext.execute(plan, jars);
                    System.out.println("Finished execution");
                } else {
                    System.out.println("Using vae cost model");
                    System.out.println("Executing query " + args[3]);
                    wayangContext.executeVAE(plan, jars);
                    System.out.println("Finished execution");
                }
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
                System.out.println("Using cost ML Model");

                break;
            case "pairwise":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-pairwise.txt");
                config.setCostModel(new PairwiseCost());

                System.out.println("Using pairwise ML Model");
                break;
            case "bvae":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-bvae.txt");

                System.out.println("Using bvae ML Model");
                break;
            case "vae":
                config.setProperty("wayang.ml.experience.enabled", "true");
                config.setProperty("wayang.ml.experience.file", experiencePath + "experience-vae.txt");

                System.out.println("Using vae ML Model");
                break;
            default:
                System.out.println("Using default cost Model");
                break;
        }

    }
}
