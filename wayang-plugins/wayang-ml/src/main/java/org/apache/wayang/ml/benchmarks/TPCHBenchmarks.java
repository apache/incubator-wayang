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
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;

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
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.core.plan.wayangplan.PlanTraversal;
import org.apache.wayang.core.plan.wayangplan.OutputSlot;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.apps.tpch.data.OrderTuple;
import org.apache.commons.lang3.ArrayUtils;

import java.util.HashMap;
import java.util.List;
import scala.collection.Seq;
import scala.collection.JavaConversions;

public class TPCHBenchmarks {

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
            Configuration config = new Configuration();
            String modelType = "";

            config.setProperty("spark.master", "spark://spark-cluster:7077");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("spark.eventLog.enabled", "true");
            config.setProperty("wayang.flink.mode.run", "distribution");
            config.setProperty("wayang.flink.parallelism", "8");
            config.setProperty("wayang.flink.master", "flink-cluster");
            config.setProperty("wayang.flink.port", "7071");
            config.setProperty("spark.app.name", "TPC-H Benchmark Query " + args[3]);
            config.setProperty("spark.executor.memory", "16g");
            config.setProperty("wayang.ml.experience.enabled", "false");

            if (args.length > 4) {
                modelType = args[4];
            }

            if (args.length > 6) {
                TPCHBenchmarks.setMLModel(config, modelType, args[5], args[6]);
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

            HashMap<String, WayangPlan> plans = TPCH.createPlans(args[1]);
            WayangPlan plan = plans.get("query" + args[3]);

            plan.collectReachableTopLevelSources().forEach(source -> {
                if (source instanceof TextFileSource) {

                    String inputUrl = ((TextFileSource) source).getInputUrl();
                    System.out.println("SAUCE: " + inputUrl);

                    if (inputUrl.equals("file:///opt/data/orders.tbl")) {
                        TextFileSource orderText = new TextFileSource(inputUrl, "UTF-8");

                        MapOperator<String, OrderTuple> orderParser = new MapOperator<>(
                            (line) -> new OrderTuple.Parser().parse(line, '|'),
                            String.class,
                            OrderTuple.class
                        );
                        orderText.connectTo(0, orderParser, 0);

                    OutputSlot.stealConnections(source.getOutput(0).getOccupiedSlots().get(0).getOwner(), orderParser);
                    }
                }
            });

            String[] jars = ArrayUtils.addAll(
                ReflectionUtils.getAllJars(TPCHBenchmarks.class),
                ReflectionUtils.getAllJars(org.apache.calcite.rel.externalize.RelJson.class)
            );

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
