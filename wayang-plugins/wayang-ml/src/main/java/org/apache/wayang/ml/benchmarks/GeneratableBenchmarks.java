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
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.flink.Flink;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.api.DataQuanta;
import org.apache.wayang.api.PlanBuilder;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.training.TPCH;
import org.apache.wayang.apps.tpch.queries.Query1Wayang;
import org.apache.wayang.apps.tpch.queries.Query3;
import org.apache.wayang.apps.tpch.queries.Query5;
import org.apache.wayang.apps.tpch.queries.Query6;
import org.apache.wayang.apps.tpch.queries.Query10;
import org.apache.wayang.apps.tpch.queries.Query12;
import org.apache.wayang.apps.tpch.queries.Query14;
import org.apache.wayang.apps.tpch.queries.Query19;
import org.apache.wayang.ml.training.GeneratableJob;
import org.apache.wayang.ml.util.Jobs;
import org.apache.wayang.api.DataQuanta;
import org.apache.commons.lang3.ArrayUtils;

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

public class GeneratableBenchmarks {

    /**
     * 0: platforms
     * 1: TPCH data set directory path
     * 2: Directory to write timings to
     * 3: query number
     * 4: model type
     * 5: model path
     * 6: experience path
     */
    public static void main(String[] args) {
        try {
            List<Plugin> plugins = JavaConversions.seqAsJavaList(Parameters.loadPlugins(args[0]));
            Class<? extends GeneratableJob> job = Jobs.getJob(Integer.parseInt(args[3]));
            System.out.println("Job: " + job.getName());
            Configuration config = new Configuration();
            String modelType = "";

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
            config.setProperty(
                "wayang.core.optimizer.pruning.strategies",
                "org.apache.wayang.core.optimizer.enumeration.TopKPruningStrategy"
            );
            config.setProperty("wayang.core.optimizer.pruning.topk", "100");

            if (args.length > 4) {
                modelType = args[4];
            }

            if (args.length > 6) {
                GeneratableBenchmarks.setMLModel(config, modelType, args[5], args[6]);
            }

            String executionTimeFile = args[2] + "query" + args[3] + "-executions";
            String optimizationTimeFile = args[2] + "query" + args[3] + "-optimizations";

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

            Constructor<?> cnstr = job.getDeclaredConstructors()[0];
            GeneratableJob createdJob = (GeneratableJob) cnstr.newInstance();
            String[] jobArgs = {args[0], args[1]};
            DataQuanta<?> quanta = createdJob.buildPlan(jobArgs);
            PlanBuilder builder = quanta.getPlanBuilder();
            WayangPlan plan = builder.build();

            //Set sink to be on Java
            //((LinkedList<Operator> )plan.getSinks()).get(0).addTargetPlatform(Java.platform());

            String[] jars = ArrayUtils.addAll(
                ReflectionUtils.getAllJars(GeneratableBenchmarks.class),
                ReflectionUtils.getAllJars(org.apache.calcite.rel.externalize.RelJson.class)
            );

            jars = ArrayUtils.addAll(
                jars,
                ReflectionUtils.getAllJars(DataQuanta.class)
            );

            /*
            FileWriter fw = new FileWriter(
                "/var/www/html/data/benchmarks/operators.txt",
                true
            );
            BufferedWriter writer = new BufferedWriter(fw);

            final Collection<Operator> operators = PlanTraversal.upstream().traverse(plan.getSinks()).getTraversedNodes();

            System.out.println("Operators: " + operators.size());

            writer.write(args[3] + ": " + operators.size());
            writer.newLine();
            writer.flush();
            writer.close();
            */
            System.out.println(modelType);
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
