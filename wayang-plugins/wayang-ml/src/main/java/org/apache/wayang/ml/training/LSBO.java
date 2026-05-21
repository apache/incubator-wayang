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

import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.optimizer.enumeration.StageAssignmentTraversal;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionStage;
import org.apache.wayang.core.plan.executionplan.ExecutionTask;
import org.apache.wayang.core.optimizer.enumeration.ExecutionTaskFlow;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.MLContext;
import org.apache.wayang.spark.Spark;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.wayang.api.python.executor.ProcessFeeder;
import org.apache.wayang.api.python.executor.ProcessReceiver;
import org.apache.wayang.api.python.executor.PythonWorkerManager;

import org.apache.wayang.apps.util.Parameters;
import org.apache.wayang.core.plugin.Plugin;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.costs.PointwiseCost;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.OrtTensorDecoder;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeDecoder;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.validation.*;
import org.apache.wayang.core.util.ExplainUtils;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.net.InetAddress;
import java.net.Socket;
import java.net.ServerSocket;
import java.io.IOException;
import java.time.Instant;
import java.time.Duration;
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
 *  -- Samples each runtime and saves the best
 *  -- Starts the retraining and replaces model
 */
public class LSBO {

    /**
     * - Create MLContext
     * - Create Job for optimization context and mappings
     * - Encode and sample
     */
    public static void process(
        WayangPlan plan,
        Configuration config,
        List<Plugin> plugins,
        String... udfJars
    ) {
        try {
            Socket socket = setupSocket();
            config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));

            WayangContext context = new WayangContext(config);
            plugins.stream().forEach(plug -> context.register(plug));
            Job samplingJob = context.createJob("sampling", plan, udfJars);
            //ExplainUtils.parsePlan(plan, false);
            samplingJob.estimateKeyFigures();
            //ExecutionPlan exPlan = samplingJob.buildInitialExecutionPlan();
            OneHotMappings.setOptimizationContext(samplingJob.getOptimizationContext());
            OneHotMappings.encodeIds = true;
            TreeNode wayangNode = TreeEncoder.encode(plan);
            //TreeNode execNode = TreeEncoder.encode(exPlan, true).withIdsFrom(wayangNode);
            String encodedInput = wayangNode.toStringEncoding() + ":" + wayangNode.toStringEncoding() + ":1";
            ArrayList<String> input = new ArrayList<>();
            input.add(encodedInput);

            ProcessFeeder<String, String> feed = new ProcessFeeder<>(
                socket,
                ByteString.copyFromUtf8("lambda x: x"),
                input
            );

            feed.send();

            // Wait for a sampled plan
            ProcessReceiver<String> r = new ProcessReceiver<>(socket);
            List<String> lsboSamples = new ArrayList<>();
            r.getIterable().iterator().forEachRemaining(lsboSamples::add);
            List<WayangPlan> decodedPlans = LSBO.decodePlans(lsboSamples, wayangNode);
            WayangPlan sampledPlan = decodedPlans.get(0);

            /*OneHotMappings
                .getInstance()
                .getPlatformsMapping()
                .forEach((key, value) -> System.out.println(key + ": " + value));*/

            // execute each WayangPlan and sample latency
            // encode the best one
            // Get the initial plan created by the LSBO loop
            //Job executionJob = context.createJob("executing", sampledPlan, udfJars);

            //ExplainUtils.parsePlan(sampledPlan, false);
            TreeNode encoded = TreeEncoder.encode(sampledPlan);
            long execTime = -1;

            try {
                Instant start = Instant.now();
                context.execute(sampledPlan, udfJars);
                Instant end = Instant.now();
                execTime = Duration.between(start, end).toMillis();

                /*
                // REMOVE THIS FOR LIVE, ONLY HERE FOR TESTING
                ExecutionPlan execPlan = context.buildInitialExecutionPlan("Test", sampledPlan, udfJars);

                execTime = (long) new Random().ints(10_000, 100_000).findFirst().getAsInt();
                */

                encodedInput = wayangNode.toStringEncoding() + ":" + encoded.toStringEncoding() + ":" + execTime;
                //System.out.println(encodedInput);

                ArrayList<String> latency = new ArrayList<>();
                latency.add(encodedInput);

                //System.out.println(input);

                ProcessFeeder<String, String> latencyFeed = new ProcessFeeder<>(
                    socket,
                    ByteString.copyFromUtf8("lambda x: x"),
                    latency
                );

                latencyFeed.send();

                System.gc();
            } catch (WayangException e) {
                e.printStackTrace();
                System.out.println(e);

                // Send longest possible time back, only execution failed
                encodedInput = wayangNode.toStringEncoding() + ":" + encoded.toStringEncoding() + ":" + -1l;
                //System.out.println(encodedInput);

                ArrayList<String> latency = new ArrayList<>();
                latency.add(encodedInput);

                System.out.println("Wayang sending: " + encodedInput);

                ProcessFeeder<String, String> latencyFeed = new ProcessFeeder<>(
                    socket,
                    ByteString.copyFromUtf8("lambda x: x"),
                    latency
                );

                latencyFeed.send();
            }

        } catch(IOException e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }

    public static void getCost(
        WayangPlan plan,
        Configuration config,
        List<Plugin> plugins,
        String... udfJars
    ) {
        try {
            Socket socket = setupSocket();
            config.load(ReflectionUtils.loadResource("wayang-api-python-defaults.properties"));

            WayangContext context = new WayangContext(config);
            plugins.stream().forEach(plug -> context.register(plug));
            Job samplingJob = context.createJob("sampling", plan, "");
            //ExplainUtils.parsePlan(plan, false);
            samplingJob.estimateKeyFigures();
            //ExecutionPlan exPlan = samplingJob.buildInitialExecutionPlan();
            OneHotMappings.setOptimizationContext(samplingJob.getOptimizationContext());
            OneHotMappings.encodeIds = true;
            TreeNode wayangNode = TreeEncoder.encode(plan);

            // Wait for a sampled plan
            ProcessReceiver<String> r = new ProcessReceiver<>(socket);
            List<String> lsboSamples = new ArrayList<>();
            r.getIterable().iterator().forEachRemaining(lsboSamples::add);
            List<WayangPlan> decodedPlans = LSBO.decodePlans(lsboSamples, wayangNode);
            WayangPlan sampledPlan = decodedPlans.get(0);

            TreeNode encoded = TreeEncoder.encode(sampledPlan);
            long execTime = -1;

            try {
                ExecutionPlan executionPlan = samplingJob.buildInitialExecutionPlan();
                TreeNode exNode = TreeEncoder.encode(executionPlan, true);
                PlanImplementation planImpl = samplingJob.getPlanImplementation();

                String encodedInput = wayangNode.toStringEncoding() + ":" + exNode.toStringEncoding() + ":" + (int) planImpl.getSquashedCostEstimate();

                ArrayList<String> latency = new ArrayList<>();
                latency.add(encodedInput);

                ProcessFeeder<String, String> latencyFeed = new ProcessFeeder<>(
                    socket,
                    ByteString.copyFromUtf8("lambda x: x"),
                    latency
                );

                latencyFeed.send();

                System.gc();
            } catch (WayangException e) {
                e.printStackTrace();
                System.out.println(e);

                // Send longest possible time back, only execution failed
                String encodedInput = wayangNode.toStringEncoding() + ":" + encoded.toStringEncoding() + ":" + -1l;
                //System.out.println(encodedInput);

                ArrayList<String> latency = new ArrayList<>();
                latency.add(encodedInput);

                System.out.println("Wayang sending: " + encodedInput);

                ProcessFeeder<String, String> latencyFeed = new ProcessFeeder<>(
                    socket,
                    ByteString.copyFromUtf8("lambda x: x"),
                    latency
                );

                latencyFeed.send();
            }

        } catch(IOException e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }

    public static List<WayangPlan> decodePlans(List<String> plans, TreeNode encoded) {
        Tuple<ArrayList<long[][]>, ArrayList<long[][]>> input = OrtTensorEncoder.encode(encoded);
        ArrayList<WayangPlan> resultPlans = new ArrayList<>();
        for (String plan: plans) {
            try {
                JSONArray jsonArray = new JSONArray(plan);
                JSONArray jsonChoices = jsonArray.getJSONArray(0);
                JSONArray jsonIndexes = jsonArray.getJSONArray(1);
                float[][][] choices = new float[1][jsonChoices.length()][jsonChoices.getJSONArray(0).length()];
                long[][][] indexes = new long[1][jsonIndexes.length()][jsonIndexes.getJSONArray(1).length()];

                for (int i = 0; i < jsonChoices.length(); i++) {
                    for (int j = 0; j < jsonChoices.getJSONArray(i).length(); j++) {
                        choices[0][i][j] = ((Double) (jsonChoices.getJSONArray(i).get(j))).floatValue();
                    }
                }

                int rows = choices[0].length;
                int cols = choices[0][0].length;
                /*
                Float[][] transposed = new Float[cols][rows];

                for (int i = 0; i < rows; i++) {
                    for (int j = 0; j < cols; j++) {
                        transposed[j][i] = choices[0][i][j];
                    }
                }*/

                for (int i = 0; i < jsonIndexes.length(); i++) {
                    for (int j = 0; j < jsonIndexes.getJSONArray(i).length(); j++) {
                        indexes[0][i][j] = ((Integer) jsonIndexes.getJSONArray(i).get(j)).longValue();
                    }
                }

                long[][] platformChoices = PlatformChoiceValidator.validate(
                    choices,
                    indexes,
                    encoded/*,
                    new BitmaskValidationRule(),
                    new OperatorValidationRule(),
                    new PostgresSourceValidationRule()*/
                );

                /*
                long[][] platformChoices = Arrays.stream(transposed)
                    .map(row -> {
                        Float max = Arrays.stream(row).max(Comparator.naturalOrder()).orElse(Float.MIN_VALUE);
                        long[] result = Arrays.stream(row)
                                .mapToLong(v -> v.equals(max) ? 1L : 0L)
                                .toArray();

                        return result;
                    })
                    .toArray(long[][]::new);
                */

                OrtTensorDecoder decoder = new OrtTensorDecoder();
                ArrayList<long[][]> mlResult = new ArrayList<long[][]>();
                mlResult.add(platformChoices);
                ArrayList<long[][]> indexList = new ArrayList<long[][]>();
                //indexList.add(input.field1.get(0));
                indexList.add(indexes[0]);

                Tuple<ArrayList<long[][]>, ArrayList<long[][]>> decoderInput = new Tuple<>(mlResult, indexList);
                TreeNode decoded = decoder.decode(decoderInput);

                // Now set the platforms on the wayangPlan
                TreeNode reconstructed = encoded.withPlatformChoicesFrom(decoded);
                WayangPlan decodedPlan = TreeDecoder.decode(reconstructed);
                System.out.println("DECODED");
                System.out.flush();

                resultPlans.add(decodedPlan);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return resultPlans;
    }

    public static Socket setupSocket() throws IOException {
        Socket socket;
        ServerSocket serverSocket;

        byte[] addr = new byte[4];
        addr[0] = 127; addr[1] = 0; addr[2] = 0; addr[3] = 1;

        /*TODO should NOT be assigned an specific port, set port as 0 (zero)*/
        serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(addr));
        serverSocket.setSoTimeout(10_000);

        // This is read from the python process to retrieve it
        System.out.println(serverSocket.getLocalPort());

        socket = serverSocket.accept();
        serverSocket.setSoTimeout(10_000);

        return socket;
    }

    public static List<Tuple2<ExecutionPlan, Double>> sampleCandidatePlans(
        WayangPlan plan,
        WayangContext context,
        int amount,
        String... udfJars
    ) {
        final Job job = context.createJob("LSBO sample", plan, udfJars);
        job.prepareWayangPlan();
        job.estimateKeyFigures();

        final Collection<PlanImplementation> executionPlans = job.enumeratePlanImplementations();
        OneHotMappings.setOptimizationContext(job.getOptimizationContext());
        OneHotMappings.encodeIds = true;

        final StageAssignmentTraversal.StageSplittingCriterion stageSplittingCriterion =
        (producerTask, channel, consumerTask) -> false;

        return executionPlans.stream()
        .limit(amount)
        .map(cand -> {
            final ExecutionTaskFlow executionTaskFlow = ExecutionTaskFlow.createFrom(cand);
            final ExecutionPlan executionPlan = ExecutionPlan.createFrom(executionTaskFlow, stageSplittingCriterion);

            return new Tuple2<>(executionPlan, cand.getSquashedCostEstimate());
        })
        .filter(tup -> tup.getField0().isSane())
        .sorted((o1, o2)-> o1.getField1().compareTo(o2.getField1()))
        .collect(Collectors.toList());
    }
}
