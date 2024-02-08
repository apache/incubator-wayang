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

import org.apache.wayang.ml.costs.MLCost;
import org.apache.wayang.ml.encoding.OneHotEncoder;
import org.apache.wayang.ml.encoding.OneHotVector;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.types.DataSetType;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.basic.operators.*;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.function.FlatMapDescriptor;
import org.apache.wayang.core.function.ReduceDescriptor;
import org.apache.wayang.core.function.TransformationDescriptor;
import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.types.DataUnitType;
import org.apache.wayang.java.Java;
import org.apache.wayang.spark.Spark;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumerator;
import org.apache.wayang.core.optimizer.enumeration.PlanEnumeration;
import org.apache.wayang.commons.util.profiledb.instrumentation.StopWatch;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.executionplan.Channel;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.basic.operators.LocalCallbackSink;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.LinkedList;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Collection;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Vector;
import java.io.BufferedWriter;
import java.io.StringWriter;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

public class JavaExecutionMLTest extends JavaExecutionTestBase {

    @Test public void testMLCostEstimate() {
        MLCost cost = new MLCost();
        PlanImplementation plan = mock(PlanImplementation.class);
        Assert.assertEquals(cost.getSquashedEstimate(plan, true), 0, 0);
    }

    @Test public void testOneHotVector() {
        OneHotVector vector = new OneHotVector();
        long[] encoded = new long[10];
        LocalCallbackSink<Integer> sink = LocalCallbackSink.createStdoutSink(Integer.class);
        vector.addOperator(encoded, sink.getClass().getName());
        Assert.assertEquals(true, true);
    }

    @Test public void testOneHotEncoding() throws IOException, URISyntaxException {
        List<Tuple2<String, Integer>> collector = new LinkedList<>();
        Configuration config = new Configuration();
        config.setCostModel(new MLCost());
        config.setProperty("wayang.ml.tuple.average-size", "100");
        WayangPlan wayangPlan = createWayangPlan("../../README.md", collector);
        WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        Collection<PlanImplementation> executionPlans = buildPlanImplementations(wayangPlan, wayangContext);

        for (PlanImplementation plan : executionPlans) {
            long[] previous = null;
            for (int i = 0; i < 10; i++) {
                long[] encoded = OneHotEncoder.encode(plan);
                if (previous != null) {
                    Assert.assertArrayEquals(previous, encoded);
                } else {
                    Assert.assertEquals(true, true);
                }
                previous = encoded;
                System.out.println(Arrays.toString(OneHotEncoder.encode(plan)));
            }
        }
    }

    private Collection<PlanImplementation> buildPlanImplementations(WayangPlan wayangPlan, WayangContext wayangContext) {
        Job job = wayangContext.createJob("encodingTestJob", wayangPlan, "");
        ExecutionPlan baseplan = job.buildInitialExecutionPlan();
        Experiment experiment = new Experiment("wayang-ml-test", new Subject("Wayang", "0.1"));
        StopWatch stopWatch = new StopWatch(experiment);
        TimeMeasurement optimizationRound = stopWatch.getOrCreateRound("optimization");
        final PlanEnumerator planEnumerator = new PlanEnumerator(wayangPlan, job.getOptimizationContext());

        final TimeMeasurement enumerateMeasurment = optimizationRound.start("Create Initial Execution Plan", "Enumerate");
        planEnumerator.setTimeMeasurement(enumerateMeasurment);
        final PlanEnumeration comprehensiveEnumeration = planEnumerator.enumerate(true);
        planEnumerator.setTimeMeasurement(null);
        optimizationRound.stop("Create Initial Execution Plan", "Enumerate");

        return comprehensiveEnumeration.getPlanImplementations();
    }

    /**
     * Creates the {@link WayangPlan} for the word count app.
     *
     * @param inputFileUrl the file whose words should be counted
     */
    private static WayangPlan createWayangPlan(String inputFileUrl, Collection<Tuple2<String, Integer>> collector) throws URISyntaxException, IOException {
        // Assignment mode: none.

        TextFileSource textFileSource = new TextFileSource(inputFileUrl);
        textFileSource.setName("Load file");

        // for each line (input) output an iterator of the words
        FlatMapOperator<String, String> flatMapOperator = new FlatMapOperator<>(
                new FlatMapDescriptor<>(line -> Arrays.asList(line.split("\\W+")),
                        String.class,
                        String.class,
                        new ProbabilisticDoubleInterval(100, 10000, 0.8)
                )
        );
        flatMapOperator.setName("Split words");

        FilterOperator<String> filterOperator = new FilterOperator<>(str -> !str.isEmpty(), String.class);
        filterOperator.setName("Filter empty words");


        // for each word transform it to lowercase and output a key-value pair (word, 1)
        MapOperator<String, Tuple2<String, Integer>> mapOperator = new MapOperator<>(
                new TransformationDescriptor<>(word -> new Tuple2<>(word.toLowerCase(), 1),
                        DataUnitType.createBasic(String.class),
                        DataUnitType.createBasicUnchecked(Tuple2.class)
                ), DataSetType.createDefault(String.class),
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        mapOperator.setName("To lower case, add counter");


        // groupby the key (word) and add up the values (frequency)
        ReduceByOperator<Tuple2<String, Integer>, String> reduceByOperator = new ReduceByOperator<>(
                new TransformationDescriptor<>(pair -> pair.field0,
                        DataUnitType.createBasicUnchecked(Tuple2.class),
                        DataUnitType.createBasic(String.class)), new ReduceDescriptor<>(
                ((a, b) -> {
                    a.field1 += b.field1;
                    return a;
                }), DataUnitType.createGroupedUnchecked(Tuple2.class),
                DataUnitType.createBasicUnchecked(Tuple2.class)
        ), DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        reduceByOperator.setName("Add counters");


        // write results to a sink
        LocalCallbackSink<Tuple2<String, Integer>> sink = LocalCallbackSink.createCollectingSink(
                collector,
                DataSetType.createDefaultUnchecked(Tuple2.class)
        );
        sink.setName("Collect result");

        // Build Rheem plan by connecting operators
        textFileSource.connectTo(0, flatMapOperator, 0);
        flatMapOperator.connectTo(0, filterOperator, 0);
        filterOperator.connectTo(0, mapOperator, 0);
        mapOperator.connectTo(0, reduceByOperator, 0);
        reduceByOperator.connectTo(0, sink, 0);

        return new WayangPlan(sink);
    }

}

