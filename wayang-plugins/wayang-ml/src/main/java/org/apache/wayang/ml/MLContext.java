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

package org.apache.wayang.ml;

import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.logging.log4j.Level;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.Job;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.plan.executionplan.ExecutionPlan;
import org.apache.wayang.core.optimizer.DefaultOptimizationContext;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.ml.costs.PairwiseCost;
import org.apache.wayang.ml.encoding.OneHotMappings;
import org.apache.wayang.ml.encoding.OrtTensorEncoder;
import org.apache.wayang.ml.encoding.TreeEncoder;
import org.apache.wayang.ml.encoding.TreeNode;
import org.apache.wayang.ml.util.EnumerationStrategy;
import org.apache.wayang.ml.util.Logging;
import org.apache.wayang.core.util.Tuple;
import org.apache.logging.log4j.Level;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.time.Instant;
import java.time.Duration;

import java.util.ArrayList;
import java.util.Optional;
import java.util.Collection;

/**
 * This is the entry point for users to work with Wayang ML.
 */
public class MLContext extends WayangContext {

    private OrtMLModel model;

    private EnumerationStrategy enumerationStrategy = EnumerationStrategy.NONE;

    public MLContext() {
        super();
    }

    public MLContext(Configuration configuration) {
        super(configuration);
    }

    /**
     * Execute a plan.
     *
     * @param wayangPlan the plan to execute
     * @param udfJars   JARs that declare the code for the UDFs
     * @see ReflectionUtils#getDeclaringJar(Class)
     */
    @Override
    public void execute(WayangPlan wayangPlan, String... udfJars) {
        this.setLogLevel(Level.ERROR);
        Job wayangJob = this.createJob("", wayangPlan, udfJars);
        OneHotMappings.setOptimizationContext(wayangJob.getOptimizationContext());

        Configuration config = this.getConfiguration();
        Configuration jobConfig = wayangJob.getConfiguration();

        wayangJob.execute();

        if (config.getBooleanProperty("wayang.ml.experience.enabled")) {
            String original;

            Optional<String> originalOption = config.getOptionalStringProperty("wayang.ml.experience.original");
            if (originalOption.isPresent()) {
                original = originalOption.get();
            } else {
                original = TreeEncoder.encode(wayangPlan).toString();
            }

            String withChoices;

            Optional<String> choicesOption = config.getOptionalStringProperty("wayang.ml.experience.with-platforms");
            if (choicesOption.isPresent()) {
                withChoices = choicesOption.get();
            } else {
                withChoices = jobConfig.getStringProperty("wayang.ml.experience.with-platforms");
            }

            long execTime = jobConfig.getLongProperty("wayang.ml.experience.exec-time");

            this.logExperience(original, withChoices, execTime);
        }
    }

    public void executeVAE(WayangPlan wayangPlan, String ...udfJars) {
        this.setLogLevel(Level.ERROR);
        try {
            Job job = this.createJob("", wayangPlan, udfJars);
            Configuration jobConfig = job.getConfiguration();
            //job.prepareWayangPlan();
            job.estimateKeyFigures();
            OneHotMappings.setOptimizationContext(job.getOptimizationContext());
            OneHotMappings.encodeIds = true;

            // Log Encoding time
            Instant start = Instant.now();
            TreeNode wayangNode = TreeEncoder.encode(wayangPlan);

            Instant end = Instant.now();
            long execTime = Duration.between(start, end).toMillis();
            Logging.writeToFile(
                String.format("Encoding: %d", execTime),
                this.getConfiguration().getStringProperty("wayang.ml.optimizations.file")
            );
            OrtMLModel model = OrtMLModel.getInstance(job.getConfiguration());
            // Log inference time
            start = Instant.now();
            Tuple<WayangPlan, TreeNode> resultTuple = model.runVAE(wayangPlan, wayangNode);
            end = Instant.now();
            execTime = Duration.between(start, end).toMillis();

            WayangPlan platformPlan = resultTuple.field0;

            this.getConfiguration().setProperty(
                "wayang.ml.experience.original",
                wayangNode.toStringEncoding()
            );

            this.getConfiguration().setProperty(
                "wayang.ml.experience.with-platforms",
                resultTuple.field1.toString()
            );

            this.execute(platformPlan, udfJars);
        } catch (Exception e) {
            e.printStackTrace();
            throw new WayangException("Executing WayangPlan with VAE model failed");
        }
    }

    public ExecutionPlan buildWithVAE(WayangPlan wayangPlan, String ...udfJars) {
        try {
            Job job = this.createJob("", wayangPlan, udfJars);
            job.estimateKeyFigures();
            OneHotMappings.setOptimizationContext(job.getOptimizationContext());
            OneHotMappings.encodeIds = true;

            TreeNode wayangNode = TreeEncoder.encode(wayangPlan);
            OrtMLModel model = OrtMLModel.getInstance(job.getConfiguration());
            Tuple<WayangPlan, TreeNode> resultTuple = model.runVAE(wayangPlan, wayangNode);
            WayangPlan platformPlan = resultTuple.field0;

            return this.buildInitialExecutionPlan("", platformPlan, udfJars);
        } catch (Exception e) {
            e.printStackTrace();
            throw new WayangException("Executing WayangPlan with VAE model failed");
        }
    }

    public void setModel(OrtMLModel model) {
        this.model = model;
    }

    private void logExperience(String original, String withChoices, long execTime) {
        if (!this.getConfiguration().getBooleanProperty("wayang.ml.experience.enabled")) {
            return;
        }

        String content = String.format("%s:%s:%d", original, withChoices, execTime);
        Logging.writeToFile(
            content,
            this.getConfiguration().getStringProperty("wayang.ml.experience.file")
        );
    }
}
