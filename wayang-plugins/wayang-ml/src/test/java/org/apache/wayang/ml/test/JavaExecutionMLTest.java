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
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ExplainUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.costs.DefaultPointwiseCost;
import org.apache.wayang.ml.encoding.OneHotMappings;
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
    @Test
    public void testOneHotVector() {
        final OneHotVector vector = new OneHotVector();
        final long[] encoded = new long[10];
        final LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        vector.addOperator(encoded, sink.getClass().getName());
        Assert.assertEquals(true, true);
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
        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);
        Assert.assertArrayEquals(encoded.encoded, encoded.encoded);
    }

    @Test
    public void testTreeDecoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String modelPath = JavaExecutionMLTest.class.getResource("/cost_model.onnx").getPath();
        System.out.println("running decoding w model path: " + modelPath);
        config.setProperty("wayang.ml.model.file", modelPath);
        config.setCostModel(DefaultPointwiseCost.FACTORY.makeCost());
        config.setProperty("wayang.ml.tuple.average-size", "100");
        final WayangPlan wayangPlan = createWayangPlan("file:///var/www/html/README.md", collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        System.out.println(ExplainUtils.parsePlan(wayangPlan, true).toString());
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        // Also encode wayang plan to set OneHotMappings.originalOperators
        final OneHotMappings mappings = new OneHotMappings();
        final TreeEncoder encoder = new TreeEncoder(mappings);
        final TreeNode wayangNode = encoder.encode(wayangPlan, wayangJob.getOptimizationContext(), true);
        final TreeDecoder decoder = new TreeDecoder(encoder);
        final WayangPlan decodedExecution = decoder.decode(wayangNode);
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
            final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
            final TreeNode encoded = encoder.encode(planImplementation);
            System.out.println(encoded);
            Assert.assertArrayEquals(encoded.encoded, encoded.encoded);
        }
    }
}
