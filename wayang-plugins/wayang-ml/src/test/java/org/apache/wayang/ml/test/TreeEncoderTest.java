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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.optimizer.enumeration.PlanImplementation;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.java.Java;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.spark.Spark;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TreeEncoderTest extends JavaExecutionTestBase {
    @Test
    public void testTreeEncoding() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());
        
        final ExecutionPlan exPlan = wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext(), false);

        Assertions.assertNotNull(exPlan);
        Assertions.assertNotNull(encoded);
        Assertions.assertEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testEncodePlanImplementationWithDefaultConfig() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> planImplementations = buildPlanImplementations(wayangPlan, wayangContext);
        Assertions.assertFalse(planImplementations.isEmpty());

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        for (final PlanImplementation planImplementation : planImplementations) {
            final TreeNode encoded = encoder.encode(planImplementation);
            Assertions.assertNotNull(encoded);
            Assertions.assertEquals(0L, encoded.encoded[0]);
        }
    }

    @Test
    public void testEncodePlanImplementationWithEncodeIdsEnabled() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Collection<PlanImplementation> planImplementations = buildPlanImplementations(wayangPlan, wayangContext);
        Assertions.assertFalse(planImplementations.isEmpty());

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        for (final PlanImplementation planImplementation : planImplementations) {
            final TreeNode encoded = encoder.encode(planImplementation);
            Assertions.assertNotNull(encoded);
            Assertions.assertNotEquals(0L, encoded.encoded[0]);
        }
    }

    @Test
    public void testEncodeWayangPlanWithConfiguration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty("wayang.ml.encode-ids", "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());

        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testTreeEncoderConstructorWithConfiguration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final WayangContext wayangContext = new WayangContext(config);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings(), config);
        Assertions.assertEquals(config, encoder.getConfiguration());

        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());
        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testMachineLearningPluginSetProperties() {
        final Configuration config = new Configuration();
        org.apache.wayang.ml.MachineLearning.plugin().setProperties(config);
        Assertions.assertFalse(config.getOptionalBooleanProperty(TreeEncoder.ENCODE_IDS_PROPERTY).isPresent());
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(config));
        Assertions.assertEquals(100L, config.getLongProperty("wayang.ml.tuple.average-size"));
    }

    @Test
    public void testPluginRegistrationPreservesUserSettings() {
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        config.setProperty("wayang.ml.model.file", "/custom/model/path.onnx");
        config.setProperty("wayang.ml.experience.enabled", "true");
        config.setProperty("wayang.ml.experience.file", "/custom/experience.txt");
        config.setProperty("wayang.core.optimizer.reoptimize", "true");
        config.setProperty("wayang.core.optimizer.reoptimize.proactive", "true");

        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(org.apache.wayang.ml.MachineLearning.plugin());
        final Configuration contextConfig = wayangContext.getConfiguration();

        // User settings must remain intact
        Assertions.assertTrue(contextConfig.getBooleanProperty(TreeEncoder.ENCODE_IDS_PROPERTY));
        Assertions.assertEquals("/custom/model/path.onnx", contextConfig.getStringProperty("wayang.ml.model.file"));
        Assertions.assertTrue(contextConfig.getBooleanProperty("wayang.ml.experience.enabled"));
        Assertions.assertEquals("/custom/experience.txt", contextConfig.getStringProperty("wayang.ml.experience.file"));
        Assertions.assertTrue(contextConfig.getBooleanProperty("wayang.core.optimizer.reoptimize"));
        Assertions.assertTrue(contextConfig.getBooleanProperty("wayang.core.optimizer.reoptimize.proactive"));

        // Unconfigured defaults should still be populated
        Assertions.assertEquals(100L, contextConfig.getLongProperty("wayang.ml.tuple.average-size"));
        Assertions.assertEquals("/var/www/html/data/executions.txt", contextConfig.getStringProperty("wayang.ml.executions.file"));
    }

    @Test
    public void testPluginRegistrationPreservesShorthandEncodeIds() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");

        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(org.apache.wayang.ml.MachineLearning.plugin());
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Configuration contextConfig = wayangContext.getConfiguration();
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(contextConfig));

        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());
        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testShorthandEncodeIdsAfterPluginRegistration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration config = new Configuration();

        final WayangContext wayangContext = new WayangContext(config);
        wayangContext.register(org.apache.wayang.ml.MachineLearning.plugin());
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        final Configuration contextConfig = wayangContext.getConfiguration();
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(contextConfig));

        // Setting shorthand after registration must not be shadowed by canonical default false
        contextConfig.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(contextConfig));

        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final Job wayangJob = wayangContext.createJob("", wayangPlan, "");
        wayangJob.buildInitialExecutionPlan();

        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings());
        final TreeNode encoded = encoder.encode(wayangPlan, wayangJob.getOptimizationContext());
        Assertions.assertNotNull(encoded);
        Assertions.assertNotEquals(0L, encoded.encoded[0]);
    }

    @Test
    public void testPluginRegistrationDirectSetPropertiesPreservesUserSettings() {
        final Configuration config = new Configuration();
        config.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        config.setProperty("wayang.ml.model.file", "/my/custom/model.onnx");
        config.setProperty("wayang.ml.experience.enabled", "true");
        config.setProperty("wayang.core.optimizer.reoptimize", "true");

        org.apache.wayang.ml.MachineLearning.plugin().setProperties(config);

        Assertions.assertEquals("/my/custom/model.onnx", config.getStringProperty("wayang.ml.model.file"));
        Assertions.assertTrue(config.getBooleanProperty("wayang.ml.experience.enabled"));
        Assertions.assertTrue(config.getBooleanProperty("wayang.core.optimizer.reoptimize"));
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(config));
        Assertions.assertEquals(100L, config.getLongProperty("wayang.ml.tuple.average-size"));
    }

    @Test
    public void testResolveEncodeIdsMatrix() {
        // 1. Both absent
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(new Configuration()));

        // 2. Canonical true, shorthand absent
        final Configuration config1 = new Configuration();
        config1.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(config1));

        // 3. Canonical false, shorthand absent
        final Configuration config2 = new Configuration();
        config2.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(config2));

        // 4. Canonical absent, shorthand true
        final Configuration config3 = new Configuration();
        config3.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(config3));

        // 5. Canonical absent, shorthand false
        final Configuration config4 = new Configuration();
        config4.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(config4));

        // 6. Canonical false, shorthand true on the same level: canonical takes precedence
        final Configuration config5 = new Configuration();
        config5.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        config5.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(config5));

        // 7. Canonical true, shorthand false
        final Configuration config6 = new Configuration();
        config6.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        config6.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(config6));

        // 8. Canonical true, shorthand true
        final Configuration config7 = new Configuration();
        config7.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        config7.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(config7));

        // 9. Canonical false, shorthand false
        final Configuration config8 = new Configuration();
        config8.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        config8.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(config8));
    }

    @Test
    public void testChildShorthandFalseOverridesInheritedCanonicalTrue() {
        final Configuration parent = new Configuration();
        parent.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final Configuration child = parent.fork();
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child));

        child.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(child));
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(parent));
    }

    @Test
    public void testChildCanonicalFalseOverridesInheritedShorthandTrue() {
        final Configuration parent = new Configuration();
        parent.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        final Configuration child = parent.fork();
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child));

        child.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(child));
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(parent));
    }

    @Test
    public void testChildTrueOverridesInheritedFalseAcrossSpellings() {
        final Configuration parent = new Configuration();
        parent.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        final Configuration child = parent.fork();
        child.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child));

        final Configuration parent2 = new Configuration();
        parent2.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        final Configuration child2 = parent2.fork();
        child2.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child2));
    }

    @Test
    public void testChildOverrideAfterCachedParentRead() {
        final Configuration parent = new Configuration();
        parent.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "true");
        final Configuration child = parent.fork();

        // Reading through the child caches the inherited value locally; this must not look like an override.
        Assertions.assertTrue(child.getBooleanProperty(TreeEncoder.ENCODE_IDS_PROPERTY));
        child.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(child));

        final Configuration parent2 = new Configuration();
        parent2.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        final Configuration child2 = parent2.fork();
        Assertions.assertTrue(child2.getBooleanProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY));
        child2.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(child2));
    }

    @Test
    public void testChildInheritsFromGrandparentWhenNotOverridden() {
        final Configuration grandparent = new Configuration();
        grandparent.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        final Configuration parent = grandparent.fork();
        final Configuration child = parent.fork();
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child));

        parent.setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(child));

        child.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(child));
    }

    @Test
    public void testJobLevelOverrideAfterPluginRegistration() throws IOException, URISyntaxException {
        final List<Tuple2<String, Integer>> collector = new LinkedList<>();
        final Configuration userConfig = new Configuration();
        userConfig.setProperty(TreeEncoder.ENCODE_IDS_SHORTHAND_PROPERTY, "true");

        final WayangContext wayangContext = new WayangContext(userConfig);
        wayangContext.register(org.apache.wayang.ml.MachineLearning.plugin());
        wayangContext.register(Java.basicPlugin());
        wayangContext.register(Spark.basicPlugin());

        // The plugin must not inject a default that shadows the user's inherited shorthand.
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(wayangContext.getConfiguration()));

        final String filePath = JavaExecutionMLTest.class.getResource("/README.md").toURI().toString();
        final WayangPlan wayangPlan = createWayangPlan(filePath, collector);
        final Job inheritingJob = wayangContext.createJob("", wayangPlan, "");
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(inheritingJob.getConfiguration()));

        final WayangPlan overridingPlan = createWayangPlan(filePath, collector);
        final Job overridingJob = wayangContext.createJob("", overridingPlan, "");
        overridingJob.getConfiguration().setProperty(TreeEncoder.ENCODE_IDS_PROPERTY, "false");
        Assertions.assertFalse(TreeEncoder.resolveEncodeIds(overridingJob.getConfiguration()));
        Assertions.assertTrue(TreeEncoder.resolveEncodeIds(wayangContext.getConfiguration()));

        overridingJob.buildInitialExecutionPlan();
        final TreeEncoder encoder = new TreeEncoder(new OneHotMappings(), overridingJob.getConfiguration());
        final TreeNode encoded = encoder.encode(overridingPlan, overridingJob.getOptimizationContext());
        Assertions.assertNotNull(encoded);
        Assertions.assertEquals(0L, encoded.encoded[0]);
    }
}

