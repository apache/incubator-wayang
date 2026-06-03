/*
 *
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

import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.costs.DefaultPointwiseCost;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Test;

public class WordCountIntegerationTest extends JavaExecutionTestBase {
    @Test
    void wordcount() throws Exception {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();

        final String modelPath = WordCountIntegerationTest.class.getResource("/cost_model.onnx").getPath();
        config.setProperty("wayang.ml.model.file", modelPath);

        config.setCostModel(new DefaultPointwiseCost.Factory().makeCost());
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangContext.execute(wayangPlan);
    }
}
