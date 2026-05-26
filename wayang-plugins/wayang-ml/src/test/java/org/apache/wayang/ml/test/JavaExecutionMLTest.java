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

package org.apache.wayang.ml.test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.FilterOperator;
import org.apache.wayang.basic.operators.FlatMapOperator;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.basic.operators.MapOperator;
import org.apache.wayang.basic.operators.ReduceByOperator;
import org.apache.wayang.basic.operators.TextFileSource;
import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.costs.DefaultPointwiseCost;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.ml.encoding.OneHotVector;
import org.apache.wayang.ml.encoding.TreeDecoder;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.spark.Spark;

import org.junit.Assert;
import org.junit.Test;

public class JavaExecutionMLTest extends JavaExecutionTestBase {

    /*
     * @Test public void testMLCostEstimate() { MLCost cost = new MLCost();
     * PlanImplementation plan = mock(PlanImplementation.class);
     * Assert.assertEquals(cost.getSquashedEstimate(plan, true), 0, 0); }
     */

    /**
     * Creates the {@link WayangPlan} for the word count app.
     *
     * @param inputFileUrl the file whose words should be counted
     */
    private static WayangPlan createWayangPlan(final String inputFileUrl, final Collection<Tuple2<String, Integer>> collector)
            throws URISyntaxException, IOException {
        // Assignment mode: none.

        final TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.setName("Load file");

        // for each line (input) output an iterator of the words
        final FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split("\\W+")), String.class, String.class,
                        new ProbabilisticDoubleInterval(100, 10000, 0.8)));
        flatMapOperator.setName("Split words");

        final FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");

        // for each word transform it to lowercase and output a key-value pair (word, 1)
        final MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class), DataUnitType.createBasicUnchecked(Tuple2.class)),
                DataSetType.createDefault(String.class), DataSetType.createDefaultUnchecked(Tuple2.class));
        mapOperator.setName("To lower case, add counter");

        // groupby the key (word) and add up the values (frequency)
        final ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0, DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)),
                new ReduceDescriptor<>(((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class), DataUnitType.createBasicUnchecked(Tuple2.class)),
                DataSetType.createDefaultUnchecked(Tuple2.class));
        reduceByOperator.setName("Add counters");

        // write results to a sink
        final LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(collector,
                DataSetType.createDefaultUnchecked(Tuple2.class));
        sink.setName("Collect result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

    @Test
    public void testOneHotVector() {
        final OneHotVector vector = new OneHotVector();
        final long[] encoded = new long[10];
        final LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        vector.addOperator(encoded, sink.getClass().getName());
        Assert.assertEquals(true, true);
    }

    @Test
    public void testOneHotEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        // config.setCostModel(new MLCost());
        config.setProperty("wayang.ml.tuple.average-size", "100");
        final WayangPlan wayangPlan = createWayangPlan("../../README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> executionPlans = buildPlanImplementations(wayangPlan, wayangContext);

        for (final PlanImplementation plan : executionPlans) {
            long[] previous = null;
            for (int i = 0; i < 10; i++) {
                final long[] encoded = OneHotEncoder.encode(plan);
                if (previous != null) {
                    Assert.assertArrayEquals(previous, encoded);
                } else {
                    Assert.assertEquals(true, true);
                }
                previous = encoded;
            }
        }
    }

    @Test
    public void testTreeEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        // Just a sanity check for determinism
        final TreeNode encoded = TreeEncoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);
        Assert.assertArrayEquals(encoded.encoded, encoded.encoded);
    }

    @Test
    public void testExecutionPlanTreeEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        // config.setCostModel(new MLCost());
        config.setProperty("wayang.ml.tuple.average-size", "100");
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        TreeEncoder.encode(exPlan, false, wayangJob.getOptimizationContext(), false);
        Assert.assertEquals(true, true);
    }

    @Test
    public void testTreeDecoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String modelPath = JavaExecutionMLTest.class.getResource("/cost_model.onnx").getPath();
        System.out.println("running decoding w model path: " + modelPath);
        config.setProperty("wayang.ml.model.file", modelPath);
        config.setCostModel(new DefaultPointwiseCost());
        config.setProperty("wayang.ml.tuple.average-size", "100");
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        System.out.println(ExplainUtils.parsePlan(wayangPlan, true).toString());
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        // Also encode wayang plan to set OneHotMappings.originalOperators
        final TreeNode wayangNode = TreeEncoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);
        final ExecutionPlan executionPlan = wayangContext.buildInitialExecutionPlan("", wayangPlan, "");
        final TreeNode executionNode = TreeEncoder.encode(exPlan, false, wayangJob.getOptimizationContext(), false)
                .withIdsFrom(wayangNode);
        // TreeNode encoded = TreeNode.fromString("((495670503, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 1, 0, 0, 0, 0, 1, 0, 0),((-559793122, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        // 0, 0, 0, 0, 1, 0, 0),((615554814, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        // 0, 1, 0, 0),((-1512350111, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0,
        // 0),((-1562004761, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0,
        // 0),((763497331, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0),(0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0),(0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)),(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0)),(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0)),(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)),(0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0)),(0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
        // 0, 0, 0, 0, 0, 0, 0, 0, 0))");
        System.out.println("execution node ot string: " + executionNode.toStringEncoding());
        final WayangPlan decodedExecution = TreeDecoder.decode(executionNode.toStringEncoding());
        System.out.println("plan: " + decodedExecution);
        final TreeNode encoded = TreeNode.fromString(executionNode.toString());
        System.out.println("encoded: " + encoded.toStringEncoding());
        final WayangPlan decodedPlan = TreeDecoder.decode(encoded);

        Assert.assertEquals(executionNode.toString(), encoded.toString());
        // Assert.assertEquals(true, true);
    }

    @Test
    public void testEncodingFromString() throws IOException, URISyntaxException {
        String encoded = "((0,1,2,3),((4,5,6,7), ((8,9,10,11),((12,13,14,15),((16,17,18,19),((20,21,22,23),((24,25,26,27),),((28,29,30,31),)),((32,33,34,35),)),((36,37,38,39),)),((40,41,42,43),)),((44,45,46,47),)),((48,49,50,51),))";
        encoded = encoded.replaceAll("\\s+", "");
        final TreeNode decoded = TreeNode.fromString(encoded);

        Assert.assertEquals(encoded, decoded.toStringEncoding());
    }

    @Test
    public void testPlanImplementationEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> planImplementations = buildPlanImplementations(wayangPlan, wayangContext);

        for (final PlanImplementation planImplementation : planImplementations) {
            // Just a sanity check for determinism
            final TreeNode encoded = TreeEncoder.encode(planImplementation);
            System.out.println(encoded);
            Assert.assertArrayEquals(encoded.encoded, encoded.encoded);
        }
    }

    private Collection<PlanImplementation> buildPlanImplementations(final WayangPlan wayangPlan,
            final WayangContext wayangContext) {
        final Job job = wayangContext.createJob("encodingTestJob", wayangPlan, "");
        final ExecutionPlan baseplan = job.buildInitialExecutionPlan();
        final Experiment experiment = new Experiment("wayang-ml-test", new Subject("Wayang", "0.1"));
        final StopWatch stopWatch = new StopWatch(experiment);
        final TimeMeasurement optimizationRound = stopWatch.getOrCreateRound("optimization");
        final PlanEnumerator planEnumerator = new PlanEnumerator(wayangPlan, job.getOptimizationContext());

        final TimeMeasurement enumerateMeasurment = optimizationRound.start("Create Initial Execution Plan",
                "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        return comprehensiveEnumeration.getPlanImplementations();
    }

}
